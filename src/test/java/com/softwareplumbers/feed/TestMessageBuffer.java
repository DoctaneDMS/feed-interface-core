/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.common.pipedstream.OutputStreamConsumer;
import com.softwareplumbers.feed.impl.buffer.MessageBuffer;
import com.softwareplumbers.feed.impl.MessageFactory;
import com.softwareplumbers.feed.impl.MessageImpl;
import com.softwareplumbers.feed.impl.buffer.BucketPool;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.json.Json;
import javax.json.JsonObject;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 *
 * @author jonathan
 */
public class TestMessageBuffer {
    
    private String asString(InputStream is) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        OutputStreamConsumer.of(()->is).consume(stream);
        return stream.toString();
    }

    private final String[] WORDS = new String[] { "sphagetti", "idle", "loves", "jane", "dog", "hair", "tantric", "slightly", "worm", "likely", "moves", "gets", "fast" };
    
    private String randomText(int count) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            builder.append(WORDS[(int)(Math.random() * WORDS.length)]);
            builder.append(" ");
        }
        return builder.toString();
    }    
    
    private Message generateMessage() {
        JsonObject testHeaders = Json.createObjectBuilder()
            .add("field1", randomText(1))
            .add("field2", randomText(1)).build();
        InputStream testData = new ByteArrayInputStream(randomText(10).getBytes());
        Instant time = Instant.now();
        FeedPath id = FeedPath.ROOT.addId(UUID.randomUUID().toString());
        try {
            return new MessageImpl(id, time, testHeaders, testData, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private int getAverageMessageSize() throws IOException {
        byte[] buffer = new byte[10000];
        int pos = 0;
        for (int i = 0; i < 10; i ++) {
            pos += generateMessage().toStream().read(buffer, pos, buffer.length - pos);
        }
        return pos/10;    
    }
    
    private TreeMap<Instant,List<Message>> generateMessages(int count, int maxPause, Consumer<Message> messageConsumer) {
        TreeMap<Instant,List<Message>> result = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            Message message = generateMessage();
            result.computeIfAbsent(message.getTimestamp(), k->new LinkedList<Message>()).add(message);
            messageConsumer.accept(message);
            int pause = (int)(Math.random() * maxPause);
            if (pause > 0) {
                try {
                    Thread.sleep(pause); // Make sure messages have different timestamps
                } catch (InterruptedException e) {
                    // don't care
                }
            }
        }
        return result;
    }
    
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
        BucketPool pool = new BucketPool(100000);
        MessageBuffer testBuffer = pool.createBuffer(1024);
        Message message = generateMessage();
        testBuffer.addMessage(message);
        Stream<Message> results = testBuffer.getMessagesAfter(message.getTimestamp().minusMillis(100));
        Message after = results.findAny().orElseThrow(()->new RuntimeException("no message"));
        assertEquals(message, after);
    }
    
    @Test
    public void testMultipleGet() throws IOException {
        int count = 80;
        BucketPool pool = new BucketPool(100000);
        MessageBuffer testBuffer = pool.createBuffer(1024);
        TreeMap<Instant,List<Message>> messages = generateMessages(count, 2, message->testBuffer.addMessage(message));
        Instant lastMessage = messages.lastKey();
        for (Instant time : messages.keySet()) {
            System.out.println("****Getting messages from " + time + " to " + lastMessage + " ****");
            Map<Instant,List<Message>> result = testBuffer.getMessagesAfter(time).collect(Collectors.groupingBy(Message::getTimestamp, Collectors.toList()));
            Map<Instant,List<Message>> shouldHaveReceived = messages.tailMap(time);
            // First check that we have the same range of timestamps
            assertEquals(shouldHaveReceived.keySet(), result.keySet());

            int maxMessagesReceivedForInstant = result.values().stream().map(List::size).max(Comparator.naturalOrder()).orElse(0);
            int maxMessagesSentForInstant = result.values().stream().map(List::size).max(Comparator.naturalOrder()).orElse(0);
            
            assertEquals(maxMessagesSentForInstant, maxMessagesReceivedForInstant);
            
            testBuffer.dumpBuffer();

            for (Instant time2 : shouldHaveReceived.keySet()) {
                Object[] sentNow = shouldHaveReceived.get(time2).toArray();
                Object[] receivedNow = result.get(time2).toArray();
                System.out.println("For " + time2 + " sent     " + Arrays.toString(sentNow));
                System.out.println("For " + time2 + " received " + Arrays.toString(receivedNow));
                assertThat(result.get(time2), containsInAnyOrder(sentNow));
            }
        }
    }
    
    @Test 
    public void testParseFromStream() throws IOException {
        MessageFactory factory = new MessageFactory();
        Message message = factory.build(FeedPath.ROOT.addId("test"), new ByteArrayInputStream("{ \"a\" : \"one\" }plus some more unstructured data".getBytes()));
        assertEquals(FeedPath.ROOT.addId("test"), message.getName());
        assertEquals("one", message.header().getString("a"));
        assertEquals("~test", message.header().getString("name"));
        assertThat(message.header().getString("timestamp"), not(isEmptyString()));
        assertThat(message.getTimestamp(), lessThanOrEqualTo(Instant.now()));
        assertThat(asString(message.getData()), equalTo("plus some more unstructured data"));
        assertThat(asString(message.getData()), equalTo("plus some more unstructured data"));
    }
    
    @Test 
    public void testParseTempFromStream() throws IOException {
        MessageFactory factory = new MessageFactory();
        Message message = factory.buildTemporary(FeedPath.ROOT.addId("test"), new ByteArrayInputStream("{ \"a\" : \"one\" }plus some more unstructured data".getBytes()));
        assertEquals(FeedPath.ROOT.addId("test"), message.getName());
        assertEquals("one", message.header().getString("a"));
        assertEquals("~test", message.header().getString("name"));
        assertThat(message.header().getString("timestamp"), not(isEmptyString()));
        assertThat(message.getTimestamp(), lessThanOrEqualTo(Instant.now()));
        assertThat(asString(message.getData()), equalTo("plus some more unstructured data"));
        //Can only use stream once
        assertThat(asString(message.getData()), equalTo(""));
    }    
    
    @Test
    public void testPoolGrowthAndDeallocation() throws IOException {
        int messageSize = getAverageMessageSize();
        BucketPool pool = new BucketPool(messageSize * 20);
        MessageBuffer buffer = pool.createBuffer(messageSize * 5);
        TreeMap<Instant, List<Message>> generated = generateMessages(40, 2, message->buffer.addMessage(message));
        // pool size should be greater than maximum
        assertThat(pool.getSize(), greaterThan(messageSize * 40L));
        buffer.dumpBuffer();
        System.out.println("deallocating");
        pool.deallocateBuckets();
        buffer.dumpBuffer();
        // pool size should be less than maximum
        assertThat(pool.getSize(), lessThanOrEqualTo(messageSize * 20L));
        TreeMap<Instant,List<Message>> result = buffer.getMessagesAfter(generated.firstKey()).collect(Collectors.groupingBy(Message::getTimestamp, ()->new TreeMap<Instant,List<Message>>(), Collectors.toList()));
        // should have lost some messages
        assertThat(result.values().stream().flatMap(List::stream).count(), lessThan(40L));
        TreeSet<Instant> lost = new TreeSet<>(generated.keySet());
        lost.removeAll(result.keySet());
        // Check that the values we have lost are all less than or equal to values that remain
        for (Instant timestamp : lost) {
            Instant less = result.floorKey(timestamp);
            assertThat(less, anyOf(nullValue(), equalTo(timestamp)));
        }
        
    }
}
