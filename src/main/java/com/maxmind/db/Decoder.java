package com.maxmind.db;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;

/*
 * Decoder for MaxMind DB data.
 *
 * This class CANNOT be shared between threads
 */
final class Decoder {

    public static final class Pos {
        private int p;

        public Pos(int offset) { p = offset; }

        public Pos set(int i) {
            this.p = i;
            return this;
        }

        public Pos inc(int i) {
            this.p += i;
            return this;
        }

        public int get() {
            return p;
        }

        public int getAndInc(int i) {
            int res = p;
            p += i;
            return res;
        }
    }

    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final int[] POINTER_VALUE_OFFSETS = {0, 0, 1 << 11, (1 << 19) + ((1) << 11), 0};

    // XXX - This is only for unit testings. We should possibly make a
    // constructor to set this
    boolean POINTER_TEST_HACK = false;

    private final NodeCache cache;

    private final long pointerBase;

    private final CharsetDecoder utfDecoder = UTF_8.newDecoder();

    private final ByteBuffer buffer;

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

    Decoder(NodeCache cache, ByteBuffer buffer, long pointerBase) {
        this.cache = cache;
        this.pointerBase = pointerBase;
        this.buffer = buffer;
    }

    private final NodeCache.Loader cacheLoader = this::decode;

    private Pos pos = new Pos(0);

    JsonNode decode(int offset) throws IOException {
        if (offset >= this.buffer.capacity()) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "pointer larger than the database.");
        }

        int save = pos.get();
        JsonNode res = decodeAt(pos.set(offset));
        pos.set(save);
        return res;
    }

    private JsonNode decodeAt(Pos pos) throws IOException {
        int ctrlByte = 0xFF & this.buffer.get(pos.getAndInc(1));

        Type type = Type.fromControlByte(ctrlByte);

        // Pointers are a special case, we don't read the next 'size' bytes, we
        // use the size to determine the length of the pointer and then follow
        // it.
        if (type.equals(Type.POINTER)) {
            int pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;
            int base = pointerSize == 4 ? (byte) 0 : (byte) (ctrlByte & 0x7);
            int packed = this.decodeIntegerAt(pos, base, pointerSize);
            long pointer = packed + this.pointerBase + POINTER_VALUE_OFFSETS[pointerSize];

            // for unit testing
            if (this.POINTER_TEST_HACK) {
                return new LongNode(pointer);
            }

            int targetOffset = (int) pointer;
            JsonNode node = cache.get(targetOffset, cacheLoader);
            return node;
        }

        if (type.equals(Type.EXTENDED)) {
            int nextByte = this.buffer.get(pos.getAndInc(1));

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
                    size = 29 + (0xFF & buffer.get(pos.getAndInc(1)));
                    break;
                case 30:
                    size = 285 + decodeIntegerAt(pos, 2);
                    break;
                default:
                    size = 65821 + decodeIntegerAt(pos, 3);
            }
        }

        return this.decodeByTypeAt(pos, type, size);
    }

    private JsonNode decodeByTypeAt(Pos pos, Type type, int size)
            throws IOException {
        switch (type) {
            case MAP:
                return this.decodeMapAt(pos, size);
            case ARRAY:
                return this.decodeArrayAt(pos, size);
            case BOOLEAN:
                return Decoder.decodeBooleanAt(pos, size);
            case UTF8_STRING:
                return new TextNode(this.decodeStringAt(pos, size));
            case DOUBLE:
                return this.decodeDoubleAt(pos, size);
            case FLOAT:
                return this.decodeFloatAt(pos, size);
            case BYTES:
                return new BinaryNode(this.getByteArrayAt(pos, size));
            case UINT16:
                return this.decodeUint16At(pos, size);
            case UINT32:
                return this.decodeUint32At(pos, size);
            case INT32:
                return this.decodeInt32At(pos, size);
            case UINT64:
            case UINT128:
                return this.decodeBigIntegerAt(pos, size);
            default:
                throw new InvalidDatabaseException(
                        "Unknown or unexpected type: " + type.name());
        }
    }

    private String decodeStringAt(Pos pos, int size) throws CharacterCodingException {
        byte[] bytes = this.getByteArrayAt(pos, size);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private IntNode decodeUint16At(Pos pos, int size) {
        return new IntNode(this.decodeIntegerAt(pos, size));
    }

    private IntNode decodeInt32At(Pos pos, int size) {
        return new IntNode(this.decodeIntegerAt(pos, size));
    }

    private long decodeLongAt(Pos pos, int size) {
        long integer = 0;
        for (int i = 0; i < size; i++) {
            integer = (integer << 8) | (this.buffer.get(pos.getAndInc(1)) & 0xFF);
        }
        return integer;
    }

    private LongNode decodeUint32At(Pos pos, int size) {
        return new LongNode(this.decodeLongAt(pos, size));
    }

    private int decodeIntegerAt(Pos pos, int size) {
        return this.decodeIntegerAt(pos, 0, size);
    }

    private int decodeIntegerAt(Pos pos, int base, int size) {
        return Decoder.decodeIntegerAt(this.buffer, pos, base, size);
    }

    static int decodeIntegerAt(ByteBuffer buffer, Pos pos, int base, int size) {
        int integer = base;
        for (int i = 0; i < size; i++) {
            integer = (integer << 8) | (buffer.get(pos.getAndInc(1)) & 0xFF);
        }
        return integer;
    }

    private BigIntegerNode decodeBigIntegerAt(Pos pos, int size) {
        byte[] bytes = this.getByteArrayAt(pos, size);
        return new BigIntegerNode(new BigInteger(1, bytes));
    }

    private DoubleNode decodeDoubleAt(Pos pos, int size) throws InvalidDatabaseException {
        if (size != 8) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "invalid size of double.");
        }
        return new DoubleNode(this.buffer.getDouble(pos.getAndInc(size)));
    }

    private FloatNode decodeFloatAt(Pos pos, int size) throws InvalidDatabaseException {
        if (size != 4) {
            throw new InvalidDatabaseException(
                    "The MaxMind DB file's data section contains bad data: "
                            + "invalid size of float.");
        }
        return new FloatNode(this.buffer.getFloat(pos.getAndInc(size)));
    }

    private static BooleanNode decodeBooleanAt(Pos pos, int size)
            throws InvalidDatabaseException {
        switch (size) {
            case 0:
                return BooleanNode.FALSE;
            case 1:
                return BooleanNode.TRUE;
            default:
                throw new InvalidDatabaseException(
                        "The MaxMind DB file's data section contains bad data: "
                                + "invalid size of boolean.");
        }
    }

    private JsonNode decodeArrayAt(Pos pos, int size) throws IOException {

        List<JsonNode> array = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            JsonNode r = this.decodeAt(pos);
            array.add(r);
        }

        return new ArrayNode(OBJECT_MAPPER.getNodeFactory(), Collections.unmodifiableList(array));
    }

    private JsonNode decodeMapAt(Pos pos, int size) throws IOException {
        int capacity = (int) (size / 0.75F + 1.0F);
        Map<String, JsonNode> map = new HashMap<>(capacity);

        for (int i = 0; i < size; i++) {
            String key = this.decodeAt(pos).asText();
            JsonNode value = this.decodeAt(pos);
            map.put(key, value);
        }

        return new ObjectNode(OBJECT_MAPPER.getNodeFactory(), Collections.unmodifiableMap(map));
    }

    private byte[] getByteArrayAt(Pos pos, int length) {
        return Decoder.getByteArrayAt(this.buffer, pos, length);
    }

    private static byte[] getByteArrayAt(ByteBuffer buffer, Pos pos, int length) {
        byte[] bytes = new byte[length];

        for (int i = 0, j = pos.get(); i < length; i++, j++)
            bytes[i] = buffer.get(j);

        pos.inc(length);

        return bytes;
    }
}
