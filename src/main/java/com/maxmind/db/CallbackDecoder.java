package com.maxmind.db;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Callback decoder for MaxMind DB data.
 *
 * This class CANNOT be shared between threads
 */
final class CallbackDecoder {

    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    private static final int[] POINTER_VALUE_OFFSETS = {0, 0, 1 << 11, (1 << 19) + ((1) << 11), 0};
    private static final int CHAR_BUFFER_SIZE = 32;

    private final CharsetDecoder utfDecoder = UTF_8.newDecoder();
    private final CharBuffer charBuffer = CharBuffer.allocate(CHAR_BUFFER_SIZE);
    private final StringBuilder stringBuffer = new StringBuilder();

    private final ByteBuffer buffer;
    private final long pointerBase;

    enum Type {
        EXTENDED, POINTER, UTF8_STRING, DOUBLE, BYTES, UINT16, UINT32, MAP, INT32, UINT64, UINT128, ARRAY, CONTAINER, END_MARKER, BOOLEAN, FLOAT;

        // Java clones the array when you call values(). Caching it increased
        // the speed by about 5000 requests per second on my machine.
        final static Type[] values = Type.values();

        static Type get(int i) throws InvalidDatabaseException {
            if (i >= Type.values.length) {
                throw new InvalidDatabaseException("The MaxMind DB file's data section contains bad data");
            }
            return Type.values[i];
        }

        private static Type get(byte b) throws InvalidDatabaseException {
            // bytes are signed, but we want to treat them as unsigned here
            return Type.get(b & 0xFF);
        }

        static Type fromControlByte(int b) throws InvalidDatabaseException {
            // The type is encoded in the first 3 bits of the byte.
            return Type.get((byte) ((0xFF & b) >>> 5));
        }
    }

    CallbackDecoder(ByteBuffer buffer, long pointerBase) {
        this.buffer = buffer;
        this.pointerBase = pointerBase;
    }


    <State> void decode(int offset, Callbacks.Callback<State> callback, State state) throws IOException {
        if (offset >= this.buffer.capacity()) {
            throw new InvalidDatabaseException(
					       "The MaxMind DB file's data section contains bad data: "
					       + "pointer larger than the database.");
        }

        this.buffer.position(offset);
        decode(callback, state);
    }

    private <State> void decode(Callbacks.Callback<State> callback, State state) throws IOException {
        int ctrlByte = 0xFF & this.buffer.get();

        Type type = Type.fromControlByte(ctrlByte);

        // Pointers are a special case, we don't read the next 'size' bytes, we
        // use the size to determine the length of the pointer and then follow
        // it.
        if (type.equals(Type.POINTER)) {
            int pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;
            int base = pointerSize == 4 ? (byte) 0 : (byte) (ctrlByte & 0x7);
            int packed = this.decodeInteger(base, pointerSize);
            long pointer = packed + this.pointerBase + POINTER_VALUE_OFFSETS[pointerSize];

            int targetOffset = (int) pointer;
            int position = buffer.position(); // Save
	    buffer.position(targetOffset);
	    decode(callback, state);
            buffer.position(position); // Restore
	    return;
        }

        if (type.equals(Type.EXTENDED)) {
            int nextByte = this.buffer.get();

            int typeNum = nextByte + 7;

            if (typeNum < 8) {
                throw new InvalidDatabaseException(
						   "Something went horribly wrong in the decoder. An extended type "
						   + "resolved to a type number < 8 (" + typeNum
						   + ")");
            }

            type = Type.get(typeNum);
        }

        int size = ctrlByte & 0x1f;
        if (size >= 29) {
            switch (size) {
	    case 29:
		size = 29 + (0xFF & buffer.get());
		break;
	    case 30:
		size = 285 + decodeInteger(2);
		break;
	    default:
		size = 65821 + decodeInteger(3);
            }
        }

        decodeByType(type, size, callback, state);
    }

    private void skip() throws IOException {
        int ctrlByte = 0xFF & this.buffer.get();

        Type type = Type.fromControlByte(ctrlByte);

        // Pointers are a special case, we don't read the next 'size' bytes, we
        // use the size to determine the length of the pointer and then follow
        // it.
        if (type.equals(Type.POINTER)) {
            int pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;
            this.skipInteger(pointerSize);
	    return;
        }

        if (type.equals(Type.EXTENDED)) {
            int nextByte = this.buffer.get();

            int typeNum = nextByte + 7;

            if (typeNum < 8) {
                throw new InvalidDatabaseException(
						   "Something went horribly wrong in the decoder. An extended type "
						   + "resolved to a type number < 8 (" + typeNum
						   + ")");
            }

            type = Type.get(typeNum);
        }

        int size = ctrlByte & 0x1f;
        if (size >= 29) {
            switch (size) {
	    case 29:
		size = 29 + (0xFF & buffer.get());
		break;
	    case 30:
		size = 285 + decodeInteger(2);
		break;
	    default:
		size = 65821 + decodeInteger(3);
            }
        }

        skipByType(type, size);
    }

    /** The output is only valid until the next time we decode a string. */
    private CharSequence decodeAsText() throws IOException {
        int ctrlByte = 0xFF & this.buffer.get();

        Type type = Type.fromControlByte(ctrlByte);

        // Pointers are a special case, we don't read the next 'size' bytes, we
        // use the size to determine the length of the pointer and then follow
        // it.
        if (type.equals(Type.POINTER)) {
            int pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;
            int base = pointerSize == 4 ? (byte) 0 : (byte) (ctrlByte & 0x7);
            int packed = this.decodeInteger(base, pointerSize);
            long pointer = packed + this.pointerBase + POINTER_VALUE_OFFSETS[pointerSize];

            int targetOffset = (int) pointer;
            int position = buffer.position(); // Save
	    buffer.position(targetOffset);
	    CharSequence result = decodeAsText();
            buffer.position(position); // Restore
	    return result;
        }

        if (type.equals(Type.EXTENDED)) {
            int nextByte = this.buffer.get();

            int typeNum = nextByte + 7;

            if (typeNum < 8) {
                throw new InvalidDatabaseException(
						   "Something went horribly wrong in the decoder. An extended type "
						   + "resolved to a type number < 8 (" + typeNum
						   + ")");
            }

            type = Type.get(typeNum);
        }

        int size = ctrlByte & 0x1f;
        if (size >= 29) {
            switch (size) {
	    case 29:
		size = 29 + (0xFF & buffer.get());
		break;
	    case 30:
		size = 285 + decodeInteger(2);
		break;
	    default:
		size = 65821 + decodeInteger(3);
            }
        }

        return decodeAsTextByType(type, size);
    }

    private <State> void decodeByType(Type type, int size, Callbacks.Callback<State> callback, State state)
            throws IOException {
        switch (type) {
	    case MAP:
		if (callback instanceof Callbacks.ObjectNode) {
		    Callbacks.ObjectNode<State> cb = (Callbacks.ObjectNode<State>)callback;
		    decodeMap(size, cb, state);
		} else {
		    skipMap(size);
		}
		return;
            case ARRAY:
		if (callback instanceof Callbacks.ArrayNode) {
		    Callbacks.ArrayNode<State> cb = (Callbacks.ArrayNode<State>)callback;
		    decodeArray(size, cb, state);
		} else {
		    skipArray(size);
		}
		return;
            case BOOLEAN:
		return; //FUT support callback.
            case UTF8_STRING:
		if (callback instanceof Callbacks.TextNode) {
		    Callbacks.TextNode<State> cb = (Callbacks.TextNode<State>)callback;
		    decodeString(size, cb, state);
		} else {
		    skipString(size);
		}
		return;
            case DOUBLE:
		if (callback instanceof Callbacks.DoubleNode) {
		    Callbacks.DoubleNode<State> cb = (Callbacks.DoubleNode<State>)callback;
		    double value = decodeDouble(size);
		    cb.setValue(state, value);
		} else {
		    skipDouble(size);
		}
 		return;
            case FLOAT:
		if (callback instanceof Callbacks.DoubleNode) {
		    Callbacks.DoubleNode<State> cb = (Callbacks.DoubleNode<State>)callback;
		    float value = decodeFloat(size);
		    cb.setValue(state, value);
		} else {
		    skipFloat(size);
		}
 		return;
            case BYTES:
		skipByteArray(size); return;
            case UINT16:
		skipInteger(size); return; //FUT support callback
            case UINT32:
		skipInteger(size); return; //FUT support callback
            case INT32:
		skipInteger(size); return; //FUT support callback
            case UINT64:
            case UINT128:
                skipBigInteger(size); return;
            default:
                throw new InvalidDatabaseException(
                        "Unknown or unexpected type: " + type.name());
        }
    }

    private CharSequence decodeAsTextByType(Type type, int size)
            throws IOException {
        switch (type) {
	    case MAP:
		skipMap(size); return "";
            case ARRAY:
		skipArray(size); return "";
            case BOOLEAN:
                return Boolean.toString(decodeBoolean(size));
            case UTF8_STRING:
		return decodeStringAsText(size);
            case DOUBLE: {
		double value = decodeDouble(size);
		stringBuffer.setLength(0);
		return stringBuffer.append(value);
	    }
            case FLOAT: {
		float value = decodeFloat(size);
		stringBuffer.setLength(0);
		return stringBuffer.append(value);
	    }
            case BYTES:
                throw new RuntimeException("Not implemented");
            case UINT16:
	    case INT32: {
		int value = decodeInteger(size);
		stringBuffer.setLength(0);
		return stringBuffer.append(value);
	    }
	    case UINT32: {
		long value = decodeLong(size);
		stringBuffer.setLength(0);
		return stringBuffer.append(value);
	    }
            case UINT64:
            case UINT128:
                throw new RuntimeException("Not implemented");
            default:
                throw new InvalidDatabaseException(
                        "Unknown or unexpected type: " + type.name());
        }
    }

    private <State> void skipByType(Type type, int size)
            throws IOException {
        switch (type) {
	    case MAP:
		skipMap(size); return;
            case ARRAY:
		skipArray(size); return;
            case BOOLEAN:
		return;
            case UTF8_STRING:
		skipString(size); return;
            case DOUBLE:
		skipDouble(size); return;
            case FLOAT:
		skipFloat(size); return;
            case BYTES:
		skipByteArray(size); return;
            case UINT16:
		skipInteger(size); return;
            case UINT32:
		skipInteger(size); return;
            case INT32:
		skipInteger(size); return;
            case UINT64:
            case UINT128:
                skipBigInteger(size); return;
            default:
                throw new InvalidDatabaseException(
                        "Unknown or unexpected type: " + type.name());
        }
    }

    private <State> void decodeString(int size, Callbacks.TextNode<State> callback, State state) throws CharacterCodingException {
        CharSequence value = decodeStringAsText(size);
        callback.setValue(state, value);
    }

    private CharSequence decodeStringAsText(int size) throws CharacterCodingException {

        int oldLimit = buffer.limit(); // Save
        buffer.limit(buffer.position() + size); // Set area to decode

	CharSequence result = decodeStringFromBuffer(buffer);

	buffer.limit(oldLimit); // Restore

	return result;
    }

    /** Decode the string. Keep result in charBuffer if it is small
     *  enough - otherwise, decode in chunks and append them to stringBuffer.
     */
    private CharSequence decodeStringFromBuffer(ByteBuffer buffer) throws CharacterCodingException {
	boolean useStringBuffer = false;
	utfDecoder.reset();
	charBuffer.clear();
	CoderResult result;
	do {
	    result = utfDecoder.decode(buffer, charBuffer, true);
	    if (result.isError()) throw new CharacterCodingException();
	    if (result == CoderResult.OVERFLOW) useStringBuffer = emptyCharBufferIntoStringBuffer(useStringBuffer);
	} while (result == CoderResult.OVERFLOW);

	result = utfDecoder.flush(charBuffer);
	if (result.isError()) throw new CharacterCodingException();
	if (result == CoderResult.OVERFLOW || useStringBuffer) {
	    useStringBuffer = emptyCharBufferIntoStringBuffer(useStringBuffer);
	    if (result == CoderResult.OVERFLOW) {
		result = utfDecoder.flush(charBuffer);
		emptyCharBufferIntoStringBuffer(useStringBuffer);
	    }
	}

	if (useStringBuffer) {
	    return stringBuffer;
	} else {
	    charBuffer.flip();
	    return charBuffer;
	}
    }

    private boolean emptyCharBufferIntoStringBuffer(boolean notFirst) {
	if (!notFirst) stringBuffer.setLength(0);
	charBuffer.flip();
	stringBuffer.append(charBuffer);
	charBuffer.clear();
	return true;
    }

    private void skipString(int size) {
	skipBytes(size);
    }

    private long decodeLong(int size) {
        long integer = 0;
        for (int i = 0; i < size; i++) {
            integer = (integer << 8) | (this.buffer.get() & 0xFF);
        }
        return integer;
    }

    private int decodeInteger(int size) {
        return this.decodeInteger(0, size);
    }

    private int decodeInteger(int base, int size) {
        return Decoder.decodeInteger(this.buffer, base, size);
    }

    static int decodeInteger(ByteBuffer buffer, int base, int size) {
        int integer = base;
        for (int i = 0; i < size; i++) {
            integer = (integer << 8) | (buffer.get() & 0xFF);
        }
        return integer;
    }

    private void skipInteger(int size) {
        skipBytes(size);
    }

    // private BigIntegerNode decodeBigInteger(int size) {
    //     byte[] bytes = this.getByteArray(size);
    //     return new BigIntegerNode(new BigInteger(1, bytes));
    // }

    private void skipBigInteger(int size) {
	skipByteArray(size);
    }

    private double decodeDouble(int size) throws InvalidDatabaseException {
        if (size != 8) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "invalid size of double.");
        }
        return this.buffer.getDouble();
    }

    private float decodeFloat(int size) throws InvalidDatabaseException {
        if (size != 4) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "invalid size of float.");
        }
        return this.buffer.getFloat();
    }

    private void skipDouble(int size) throws InvalidDatabaseException {
        if (size != 8) {
            throw new InvalidDatabaseException(
					       "The MaxMind DB file's data section contains bad data: "
					       + "invalid size of double.");
        }
	skipBytes(8);
    }

    private void skipFloat(int size) throws InvalidDatabaseException {
        if (size != 4) {
            throw new InvalidDatabaseException(
					       "The MaxMind DB file's data section contains bad data: "
					       + "invalid size of float.");
        }
	skipBytes(4);
    }

    private static boolean decodeBoolean(int size)
            throws InvalidDatabaseException {
        switch (size) {
            case 0:
                return false;
            case 1:
                return true;
            default:
                throw new InvalidDatabaseException(
                        "The MaxMind DB file's data section contains bad data: "
                                + "invalid size of boolean.");
        }
    }

    private <State> void decodeArray(int size, Callbacks.ArrayNode<State> callback, State state) throws IOException {
	callback.arrayBegin(state, size);
        for (int i = 0; i < size; i++) {
	    Callbacks.Callback<State> elementCallback = callback.callbackForElement(state, i, size);
	    if (elementCallback != null) {
		decode(elementCallback, state); // Value is of interest.
	    } else {
		skip();
	    }
	}
	callback.arrayEnd(state);
    }

    private void skipArray(int size) throws IOException {
        for (int i = 0; i < size; i++) {
	    skip();
        }
    }


    private <State> void decodeMap(int size, Callbacks.ObjectNode<State> callback, State state) throws IOException {
	callback.objectBegin(state);
        for (int i = 0; i < size; i++) {
	    CharSequence key = this.decodeAsText();
	    Callbacks.Callback<State> fieldCallback = callback.callbackForField(key);
	    if (fieldCallback != null) {
		decode(fieldCallback, state); // Value is of interest.
	    } else {
		skip();
	    }
	}
	callback.objectEnd(state);
    }

    private void skipMap(int size) throws IOException {
        for (int i = 0; i < size; i++) {
            skip(); // key
            skip(); // value
        }
    }

    private void skipByteArray(int length) {
        skipBytes(length);
    }

    private void skipBytes(int size) {
        buffer.position(buffer.position() + size);
    }
}
