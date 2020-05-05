package com.maxmind.db;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

public class MultiThreadedTest {

    @Test
    public void multipleMmapOpens() throws Exception {
        Callable<Callable<JsonNode>> task = () -> () -> {
            try (Reader reader = new Reader(ReaderTest.getFile("MaxMind-DB-test-decoder.mmdb"))) {
                return reader.get(InetAddress.getByName("::1.1.1.0"));
            }
        };
        MultiThreadedTest.runThreads(task);
    }

    @Test
    public void streamThreadTest() throws Exception {
        try (Reader reader = new Reader(ReaderTest.getStream("MaxMind-DB-test-decoder.mmdb"))) {
            MultiThreadedTest.threadTest(reader);
        }
    }

    @Test
    public void mmapThreadTest() throws Exception {
        try (Reader reader = new Reader(ReaderTest.getFile("MaxMind-DB-test-decoder.mmdb"))) {
            MultiThreadedTest.threadTest(reader);
        }
    }

    private static void threadTest(final Reader sharedReader)
            throws Exception {
        Callable<Callable<JsonNode>> task = () -> {
            Reader reader = sharedReader.threadSafeCopy();
            return () -> reader.get(InetAddress.getByName("::1.1.1.0"));
        };
        MultiThreadedTest.runThreads(task);
    }

    private static void runThreads(Callable<Callable<JsonNode>> taskMaker)
            throws Exception {
        int threadCount = 256;
        List<Callable<JsonNode>> tasks = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            tasks.add( taskMaker.call() );
        }
        ExecutorService executorService = Executors
                .newFixedThreadPool(threadCount);
        List<Future<JsonNode>> futures = executorService.invokeAll(tasks);

        for (Future<JsonNode> future : futures) {
            JsonNode record = future.get();
            assertEquals(268435456, record.get("uint32").intValue());
            assertEquals("unicode! ☯ - ♫", record.get("utf8_string")
                    .textValue());
        }
    }
}
