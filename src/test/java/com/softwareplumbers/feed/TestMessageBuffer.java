/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.impl.buffer.MessageBuffer;
import com.softwareplumbers.feed.impl.MessageFactory;
import com.softwareplumbers.feed.impl.MessageImpl;
import com.softwareplumbers.feed.impl.buffer.BufferPool;
import com.softwareplumbers.feed.test.TestUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.json.Json;
import javax.json.JsonObject;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;

import static com.softwareplumbers.feed.test.TestUtils.*;
import java.util.Collections;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 *
 * @author jonathan
 */
public class TestMessageBuffer {
    

    
    @Test
    public void testSimpleMessage() throws IOException {
        JsonObject testHeaders = Json.createObjectBuilder().add("field1", "one").build();
        InputStream testData = new ByteArrayInputStream("abc123".getBytes());
        Instant time = Instant.now();
        FeedPath id = FeedPath.ROOT.addId("123");
        Message message = new MessageImpl(id, time, testHeaders, testData, true);
        assertEquals("123", message.getId());
        assertEquals(FeedPath.ROOT.addId("123"), message.getName());
        assertEquals(time, message.getTimestamp());
        assertEquals("abc123", asString(message.getData()));        
    }

    @Test
    public void testTemporaryMessage() throws IOException {
        JsonObject testHeaders = Json.createObjectBuilder().add("field1", "one").build();
        InputStream testData = new ByteArrayInputStream("abc123".getBytes());
        Instant time = Instant.now();
        FeedPath id = FeedPath.ROOT.addId("123");
        Message message = new MessageImpl(id, time, testHeaders, testData, true);
        assertEquals("123", message.getId());
        assertEquals(FeedPath.ROOT.addId("123"), message.getName());
        assertEquals(time, message.getTimestamp());
        assertEquals("abc123", asString(message.getData()));
        assertEquals(-1, message.getData().read());
    }

    @Test
    public void testPersistentMessage() throws IOException {
        JsonObject testHeaders = Json.createObjectBuilder().add("field1", "one").build();
        InputStream testData = new ByteArrayInputStream("abc123".getBytes());
        Instant time = Instant.now();
        FeedPath id = FeedPath.ROOT.addId("123");
        Message message = new MessageImpl(id, time, testHeaders, testData, false);
        assertEquals("123", message.getId());
        assertEquals(FeedPath.ROOT.addId("123"), message.getName());
        assertEquals(time, message.getTimestamp());
        assertEquals("abc123", asString(message.getData()));
        assertEquals("abc123", asString(message.getData()));
    }
    
    @Test
    public void testMessageRoundtrip() throws IOException {
        BufferPool pool = new BufferPool(100000);
        MessageBuffer testBuffer = pool.createBuffer(1024);
        Message message = generateMessage(randomFeedPath());
        testBuffer.addMessage(message);
        MessageIterator results = testBuffer.getMessagesAfter(message.getTimestamp().minusMillis(100));
        Message after = results.toStream().findAny().orElseThrow(()->new RuntimeException("no message"));
        assertThat(after.getTimestamp(), greaterThanOrEqualTo(message.getTimestamp()));
        assertEquals(message, after);
        assertThat(asString(after.getData()), equalTo(asString(message.getData())));
    }
    
    @Test
    public void testMultipleGet() throws IOException, InterruptedException {
        int count = 80;
        BufferPool pool = new BufferPool(100000);
        MessageBuffer testBuffer = pool.createBuffer(1024);
        Instant first = Instant.now();
        Thread.sleep(100);
        Map<FeedPath,Message> messages = generateMessages(count, 2, randomFeedPath(), message->testBuffer.addMessage(message));
        testBuffer.dumpBuffer();
        System.out.println("****Getting messages from " + first + " ****");
        List<Message> result = testBuffer.getMessagesAfter(first).toStream().collect(Collectors.toList());
        
        assertEquals(80, result.size());
        for (int i = 0; i < 80; i++) {
            Message received = result.get(i);
            Message sent = messages.get(received.getName());
            System.out.println("checking " + received + " against " + sent);
            assertThat(sent.getHeaders(), equalTo(received.getHeaders()));
            assertThat(asString(received.getData()), equalTo(asString(sent.getData())));
        }
            
        for (int i = 0; i < count; i++)  {
            first = result.get(i).getTimestamp();
            System.out.println("****Getting messages from " + first + " ****");
            long newGet = testBuffer.getMessagesAfter(first).toStream().count();
            assertEquals(79-i, newGet);
        }       
    }
    
    @Test 
    public void testParseFromStream() throws IOException {
        MessageFactory factory = new MessageFactory();
        Message message = factory.build(FeedPath.ROOT.addId("test"), new ByteArrayInputStream("{ \"a\" : \"one\" }plus some more unstructured data".getBytes()));
        assertEquals("one", message.getHeaders().getString("a"));
        assertEquals(FeedPath.ROOT.addId("test"), message.getName());
        assertThat(message.getTimestamp(), lessThanOrEqualTo(Instant.now()));
        assertThat(asString(message.getData()), equalTo("plus some more unstructured data"));
        assertThat(asString(message.getData()), equalTo("plus some more unstructured data"));
    }
    
    @Test 
    public void testParseTempFromStream() throws IOException {
        MessageFactory factory = new MessageFactory();
        Message message = factory.buildTemporary(FeedPath.ROOT.addId("test"), new ByteArrayInputStream("{ \"a\" : \"one\" }plus some more unstructured data".getBytes()));
        assertEquals(FeedPath.ROOT.addId("test"), message.getName());
        assertEquals("one", message.getHeaders().getString("a"));
        assertThat(message.getTimestamp(), lessThanOrEqualTo(Instant.now()));
        assertThat(asString(message.getData()), equalTo("plus some more unstructured data"));
        //Can only use stream once
        assertThat(asString(message.getData()), equalTo(""));
    }    
    
    @Test
    public void testPoolGrowthAndDeallocation() throws IOException, InterruptedException {
        int messageSize = getAverageMessageSize();
        BufferPool pool = new BufferPool(messageSize * 20);
        MessageBuffer buffer = pool.createBuffer(messageSize * 5);
        Instant first = Instant.now();
        Thread.sleep(100);
        Map<FeedPath,Message> generated = generateMessages(40, 2, randomFeedPath(), message->buffer.addMessage(message));
        // pool size should be greater than maximum
        assertThat(pool.getSize(), greaterThan(messageSize * 40L));
        buffer.dumpBuffer();
        System.out.println("deallocating");
        pool.deallocateBuckets();
        buffer.dumpBuffer();
        // pool size should be less than maximum
        assertThat(pool.getSize(), lessThanOrEqualTo(messageSize * 20L));
        List<Message> result = buffer.getMessagesAfter(first).toStream().collect(Collectors.toList());
        // should have lost some messages
        assertThat(result.size(), lessThan(40));
    }
    
    @Test
    public void testMulthreadedAdd() throws IOException, InterruptedException {
        BufferPool pool = new BufferPool(1000000);
        MessageBuffer buffer = pool.createBuffer(2000);
        Instant start = Instant.now();
        Thread.sleep(100);
        Map<FeedPath,Message> generated = generateMessages(5, 20, 5, getFeeds(), message->buffer.addMessage(message)); 
        int receivedCount = 0;
        while (receivedCount < 100) {
            Thread.sleep(5);
            try (MessageIterator messages = buffer.getMessagesAfter(start)) {
                while (messages.hasNext()) {
                    Message received = messages.next();
                    receivedCount++;
                    start = received.getTimestamp();
                    assertThat(generated, hasEntry(received.getName(), received));
                    generated.remove(received.getName());
                }
            }
        }    
    }
       
    @Test
    public void testCallback() throws InterruptedException {
        BufferPool pool = new BufferPool(1000000);
        MessageBuffer buffer = pool.createBuffer(2000);
        Instant start = Instant.now();
        Thread.sleep(100);
        Map<FeedPath,Message> generated = generateMessages(5, 20, 5, Collections.singletonList(randomFeedPath()), message->buffer.addMessage(message));         
        Map<FeedPath,Message> results = new TreeMap<>();
        CountDownLatch receiving = new CountDownLatch(1);
        createReceiver(buffer, 100, start, results, receiving);
        if (receiving.await(20, TimeUnit.SECONDS)) {
            assertMapsEqual(generated, results);
        } else {
            buffer.dumpBuffer();
            showMissing(generated, results);
            fail("poll timed out");            
        }
    }
    
    @Test
    public void testCallbackMultipleReceivers() throws InterruptedException {
        BufferPool pool = new BufferPool(1000000);
        MessageBuffer buffer = pool.createBuffer(2000);
        Instant start = Instant.now();
        Thread.sleep(100);
        Map<FeedPath,Message> generated = generateMessages(5, 20, 5, getFeeds(), message->buffer.addMessage(message)); 
        CountDownLatch receiving = new CountDownLatch(3);
        List<Map<FeedPath,Message>> results = createReceivers(receiving, buffer, start, 100);
        if (receiving.await(20, TimeUnit.SECONDS)) {
            for (Map<FeedPath,Message> resultMap : results) {
                assertMapsEqual(generated, resultMap);
            }
        } else {
            buffer.dumpBuffer();
            for (Map<FeedPath,Message> resultMap : results) {
                showMissing(generated, resultMap);
            }
            fail("receivers timed out");            
        }
    }

}
