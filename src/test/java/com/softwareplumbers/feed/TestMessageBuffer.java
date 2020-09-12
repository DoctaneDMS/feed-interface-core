/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.impl.buffer.MessageBuffer;
import com.softwareplumbers.feed.impl.MessageImpl;
import com.softwareplumbers.feed.impl.buffer.BufferPool;
import com.softwareplumbers.feed.impl.buffer.MessageClock;
import com.softwareplumbers.feed.test.TestUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import static org.hamcrest.MatcherAssert.assertThat;
import org.hamcrest.Matchers;

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
        Optional<UUID> serverId = Optional.of(UUID.randomUUID());
        FeedPath id = FeedPath.ROOT.addId("123");
        Message message = new MessageImpl(MessageType.NONE, id, "testuser", time, serverId, Optional.empty(), testHeaders, testData, -1, true);
        assertEquals("123", message.getId());
        assertEquals(FeedPath.ROOT.addId("123"), message.getName());
        assertEquals(time, message.getTimestamp());
        assertEquals(serverId, message.getServerId());
        assertEquals("abc123", asString(message.getData()));   
        assertEquals("testuser", message.getSender());
    }

    @Test
    public void testTemporaryMessage() throws IOException {
        JsonObject testHeaders = Json.createObjectBuilder().add("field1", "one").build();
        InputStream testData = new ByteArrayInputStream("abc123".getBytes());
        Instant time = Instant.now();
        Optional<UUID> serverId = Optional.of(UUID.randomUUID());
        FeedPath id = FeedPath.ROOT.addId("123");
        Message message = new MessageImpl(MessageType.NONE, id, "testuser", time, serverId, Optional.empty(), testHeaders, testData, -1, true);
        assertEquals("123", message.getId());
        assertEquals(FeedPath.ROOT.addId("123"), message.getName());
        assertEquals(time, message.getTimestamp());
        assertEquals(serverId, message.getServerId());
        assertEquals("abc123", asString(message.getData()));
        assertEquals(-1, message.getData().read());
        assertEquals("testuser", message.getSender());
    }

    @Test
    public void testPersistentMessage() throws IOException {
        JsonObject testHeaders = Json.createObjectBuilder().add("field1", "one").build();
        InputStream testData = new ByteArrayInputStream("abc123".getBytes());
        Instant time = Instant.now();
        Optional<UUID> serverId = Optional.of(UUID.randomUUID());
        FeedPath id = FeedPath.ROOT.addId("123");
        Message message = new MessageImpl(MessageType.NONE, id, "testuser", time, serverId, Optional.empty(), testHeaders, testData, -1, false);
        assertEquals("123", message.getId());
        assertEquals(FeedPath.ROOT.addId("123"), message.getName());
        assertEquals(time, message.getTimestamp());
        assertEquals(serverId, message.getServerId());
        assertEquals("abc123", asString(message.getData()));
        assertEquals("abc123", asString(message.getData()));
        assertEquals("testuser", message.getSender());
    }
    
    @Test
    public void testMessageRoundtrip() throws IOException {
        BufferPool pool = new BufferPool(100000);
        MessageBuffer testBuffer = pool.createBuffer(new MessageClock(), 1024);
        Message message = generateMessage(randomFeedPath()).setServerId(UUID.randomUUID());
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
        MessageBuffer testBuffer = pool.createBuffer(new MessageClock(), 1024);
        Instant first = Instant.now();
        Thread.sleep(100);
        Map<FeedPath,Message> messages = generateMessages(count, 2, randomFeedPath(), message->testBuffer.addMessage(message.setServerId(UUID.randomUUID()))).collect(Collectors.toMap(m->m.getName(), m->m));
        testBuffer.dumpState(new PrintWriter(System.out));
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
    public void testPoolGrowthAndDeallocation() throws IOException, InterruptedException {
        int messageSize = getAverageMessageSize();
        BufferPool pool = new BufferPool(messageSize * 5);
        MessageBuffer buffer = pool.createBuffer(new MessageClock(), 1024);
        Instant first = Instant.now();
        Thread.sleep(100);
        Stream<Message> messages = generateMessages(40, 2, randomFeedPath(), message->buffer.addMessage(message.setServerId(UUID.randomUUID())));
        // pool size should be greater than maximum
        assertThat(pool.getSize(), greaterThan(messageSize * 20L));
        buffer.dumpState(new PrintWriter(System.out));
        System.out.println("deallocating");
        pool.deallocateBuckets();
        buffer.dumpState(new PrintWriter(System.out));
        // pool size should be less than maximum
        assertThat(pool.getSize(), lessThanOrEqualTo(messageSize * 20L));
        List<Message> result = buffer.getMessagesAfter(first).toStream().collect(Collectors.toList());
        // should have lost some messages
        assertThat(result.size(), lessThan(40));
    }
    
    @Test
    public void testMulthreadedAdd() throws IOException, InterruptedException, ExecutionException {
        BufferPool pool = new BufferPool(100000);
        MessageBuffer buffer = pool.createBuffer(new MessageClock(), 1024);
        Instant start = Instant.now();
        Thread.sleep(100);
        CompletableFuture<Stream<Message>> generated = generateMessages(5, 20, 5, getFeeds(), message->buffer.addMessage(message.setServerId(UUID.randomUUID()))); 
        int receivedCount = 0;
        ArrayList<Message> receiveds = new ArrayList<>(100);
        while (receivedCount < 100) {
            Thread.sleep(5);
            try (MessageIterator messages = buffer.getMessagesAfter(start)) {
                while (messages.hasNext()) {
                    Message received = messages.next();
                    receiveds.add(received);
                    receivedCount++;
                    start = received.getTimestamp();
                }
            }
        }    
        assertMatch(generated.get(), receiveds.stream());
    }
       
    @Test
    public void testCallback() throws InterruptedException, ExecutionException {
        BufferPool pool = new BufferPool(100000);
        MessageBuffer buffer = pool.createBuffer(new MessageClock(), 1024);
        Instant start = Instant.now();
        Thread.sleep(100);
        CompletableFuture<Stream<Message>> generated = generateMessages(5, 20, 5, Collections.singletonList(randomFeedPath()), message->buffer.addMessage(message.setServerId(UUID.randomUUID())));         
        Map<FeedPath,Message> results = new TreeMap<>();
        createReceiver(1, buffer, 100, start, results);
        TestUtils.assertMatch(generated.get(), results.values().stream());
    }
    
    @Test
    public void testCallbackMultipleReceivers() throws InterruptedException, ExecutionException, TimeoutException {
        BufferPool pool = new BufferPool(100000);
        MessageBuffer buffer = pool.createBuffer(new MessageClock(), 1024);
        Instant start = Instant.now();
        Thread.sleep(100);
        CompletableFuture<List<Message>> generated = generateMessages(5, 20, 5, getFeeds(), message->buffer.addMessage(message.setServerId(UUID.randomUUID()))).thenApply(stream->stream.collect(Collectors.toList())); 
        CountDownLatch receiving = new CountDownLatch(3);
        List<Map<FeedPath,Message>> results = createReceivers(receiving, buffer, start, 100);
        if (receiving.await(20, TimeUnit.SECONDS)) {
            for (Map<FeedPath,Message> resultMap : results) {
                assertMatch(generated.get(10, TimeUnit.SECONDS).stream(), resultMap.values().stream());
            }
        } else {
            dumpThreads();
            buffer.dumpState(new PrintWriter(System.out));
            for (Map<FeedPath,Message> resultMap : results) {
                showDifference(getDifference(generated.get(10, TimeUnit.SECONDS).stream(), resultMap.values().stream()));
            }
            fail("receivers timed out");            
        }
    }

}
