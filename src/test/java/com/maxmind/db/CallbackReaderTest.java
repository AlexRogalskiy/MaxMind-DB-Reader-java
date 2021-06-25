package com.maxmind.db;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.BiFunction;

import static org.junit.Assert.*;

import com.maxmind.db.Callbacks.*;

public class CallbackReaderTest {
    static class GetRecordTest {
        InetAddress ip;
        File db;
        String network;
        boolean hasRecord;

        GetRecordTest(String ip, String file, String network, boolean hasRecord) throws UnknownHostException {
            this.ip = InetAddress.getByName(ip);
            db = getFile(file);
            this.network = network;
            this.hasRecord = hasRecord;
        }
    }

    @Test
    public void testDecodingTypesFile() throws IOException {
        CallbackReader testReader = new CallbackReader(getFile("MaxMind-DB-test-decoder.mmdb"));
        this.testDecodingTypes(testReader);
    }

    @Test
    public void testDecodingTypesStream() throws IOException {
        CallbackReader testReader = new CallbackReader(getStream("MaxMind-DB-test-decoder.mmdb"));
        this.testDecodingTypes(testReader);
    }

    private void testDecodingTypes(CallbackReader reader) throws IOException {
	final AllocMeter allocMeter = new AllocMeter();
	InetAddress key = InetAddress.getByName("::1.1.1.0");
	byte[] rawKey = key.getAddress();

	BiFunction<String, RecordCallbackBuilder<AccumulatorForTypes>, AccumulatorForTypes> runIt = new BiFunction<String, RecordCallbackBuilder<AccumulatorForTypes>, AccumulatorForTypes>() {
		public AccumulatorForTypes apply(String runDescription, RecordCallbackBuilder<AccumulatorForTypes> builder) {
		    RecordCallback<AccumulatorForTypes> callback = builder.build();
		    AccumulatorForTypes acc = new AccumulatorForTypes();
		    try {
			long a0 = allocMeter.allocByteCount();
			reader.lookupRecord(rawKey, callback, acc);
			long a1 = allocMeter.allocByteCount();

			long allocatedBytes = a1 - a0;
			//System.out.println("Allocation by lookup call for '" + runDescription + "': "+ allocatedBytes);

			if (!"Warmup".equals(runDescription)) {
			    assertTrue("Zero-allocation broken by '"+runDescription+"': allocatedBytes = "+allocatedBytes,
				       allocatedBytes == 0);
			}
		    } catch (IOException ioe) {
			throw new RuntimeException(ioe);
		    }
		    return acc;
		}
	    };

	// Warm up the reader:
	for (int i=0; i<100; i++) {
	    runIt.apply("Warmup", new RecordCallbackBuilder<AccumulatorForTypes>());
	}
	System.gc(); // Try to stabilize.
	Thread.yield(); // Allow for JIT to kick in.

	{ // Strings:
	    RecordCallbackBuilder<AccumulatorForTypes> builder = new RecordCallbackBuilder<>();
	    builder.text("utf8_string", (AccumulatorForTypes state, CharSequence value) -> TextNode.assignToStringBuilder(state.string1, value));
	    AccumulatorForTypes result = runIt.apply("String value", builder);
	    assertEquals("unicode! ☯ - ♫", result.string1.toString());
	}

	{ // Integer numbers A:
	    RecordCallbackBuilder<AccumulatorForTypes> builder = new RecordCallbackBuilder<>();
	    builder.integer("uint16", (AccumulatorForTypes state, long value) -> state.long1 = value);
	    builder.integer("int32", (AccumulatorForTypes state, long value) -> state.long2 = value);
	    AccumulatorForTypes result = runIt.apply("Number values", builder);
	    assertEquals(100, result.long1);
	    assertEquals(-268435456, result.long2);
	}
	{ // Integer numbers B:
	    RecordCallbackBuilder<AccumulatorForTypes> builder = new RecordCallbackBuilder<>();
	    builder.integer("uint32", (AccumulatorForTypes state, long value) -> state.long1 = value);
	    AccumulatorForTypes result = runIt.apply("Number values", builder);
	    assertEquals(268435456L, result.long1);
	}

	{ // Floating-point numbers:
	    RecordCallbackBuilder<AccumulatorForTypes> builder = new RecordCallbackBuilder<>();
	    builder.number("double", (AccumulatorForTypes state, double value) -> state.double1 = value);
	    builder.number("float", (AccumulatorForTypes state, double value) -> state.double2 = value);
	    AccumulatorForTypes result = runIt.apply("Number values", builder);
	    assertEquals(42.123456, result.double1, 0.000000001);
	    assertEquals(1.1, result.double2, 0.000001);
	}

	{ // Nested records:
	    RecordCallbackBuilder<AccumulatorForTypes> builder = new RecordCallbackBuilder<>();
	    builder.obj("map").obj("mapX").text("utf8_stringX", (AccumulatorForTypes state, CharSequence value) -> TextNode.assignToStringBuilder(state.string1, value));
	    AccumulatorForTypes result = runIt.apply("Value in nested records", builder);
	    assertEquals("hello", result.string1.toString());
	}

	{ // Arrays - 1:
	    LongNode<AccumulatorForTypes> evenElementCallback = (AccumulatorForTypes state, long value) -> state.string1.append("(Even:").append(value).append(")");
	    LongNode<AccumulatorForTypes> oddElementCallback = (AccumulatorForTypes state, long value) -> state.string1.append("(Odd:").append(value).append(")");

	    RecordCallbackBuilder<AccumulatorForTypes> builder = new RecordCallbackBuilder<>();
	    builder.array("array",
			  (AccumulatorForTypes state, int size) ->  state.string1.append("(Start:").append(size).append(")"),
			  (AccumulatorForTypes state, int index, int size) -> { state.string1.append("(Index:").append(index).append("/").append(size).append(")"); return index % 2 == 0 ? evenElementCallback : oddElementCallback; },
			  (AccumulatorForTypes state) ->  state.string1.append("(End)")
			  );
	    runIt.apply("Warmup", builder); // Resolve dynamic methods?
	    AccumulatorForTypes result = runIt.apply("Array on top-level", builder);
	    assertEquals("(Start:3)(Index:0/3)(Even:1)(Index:1/3)(Odd:2)(Index:2/3)(Even:3)(End)", result.string1.toString());
	}
	{ // Arrays - 2:
	    LongNode<AccumulatorForTypes> evenElementCallback = (AccumulatorForTypes state, long value) -> state.string1.append("(Even:").append(value).append(")");
	    LongNode<AccumulatorForTypes> oddElementCallback = (AccumulatorForTypes state, long value) -> state.string1.append("(Odd:").append(value).append(")");

	    RecordCallbackBuilder<AccumulatorForTypes> builder = new RecordCallbackBuilder<>();
	    builder.obj("map").obj("mapX").array("arrayX",
			  (AccumulatorForTypes state, int size) ->  state.string1.append("(Start:").append(size).append(")"),
			  (AccumulatorForTypes state, int index, int size) -> { state.string1.append("(Index:").append(index).append("/").append(size).append(")"); return index % 2 == 0 ? evenElementCallback : oddElementCallback; },
			  (AccumulatorForTypes state) ->  state.string1.append("(End)")
			  );
	    runIt.apply("Warmup", builder); // Resolve dynamic methods?
	    AccumulatorForTypes result = runIt.apply("Array in sub-object", builder);
	    assertEquals("(Start:3)(Index:0/3)(Even:7)(Index:1/3)(Odd:8)(Index:2/3)(Even:9)(End)", result.string1.toString());
	}

	// Node type not handled yet: "boolean"
	// Node type not handled yet: "bytes"
	// Node type not handled yet: "map" - existence signal
	// Node type not handled yet: "uint64"/"int128"
    }

    static class AccumulatorForTypes {
	StringBuilder string1 = new StringBuilder(1000);
	long long1 = 0;
	long long2 = 0;
	double double1 = Double.NaN;
	double double2 = Double.NaN;
	{
	    string1.append("test\u1010").setLength(0); // Ensure buffer is primed for non-Latin1 content.
	}
    }


    static File getFile(String name) {
        return new File(ReaderTest.class.getResource("/maxmind-db/test-data/" + name).getFile());
    }

    static InputStream getStream(String name) {
        return ReaderTest.class.getResourceAsStream("/maxmind-db/test-data/" + name);
    }

    static class Accumulator {
	byte[] ipAddress;
	int prefixLength;
	boolean recordFound;

	final StringBuilder continent = new StringBuilder();
	final StringBuilder country = new StringBuilder();
	final StringBuilder city = new StringBuilder();
	double latitude, longitude;

	public void reset() {
	    ipAddress = null;
	    prefixLength = -1;
	    recordFound = false;

	    city.setLength(0);
	    continent.setLength(0);
	    country.setLength(0);
	    latitude = Double.NaN;
	    longitude = Double.NaN;
	}

	public Network getNetwork() {
	    try {
		return new Network(InetAddress.getByAddress(ipAddress), prefixLength);
	    } catch (UnknownHostException uhe) {throw new RuntimeException(uhe);}
	}
    }

}
