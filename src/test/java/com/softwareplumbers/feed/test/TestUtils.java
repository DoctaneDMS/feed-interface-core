/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.test;

import com.softwareplumbers.common.pipedstream.OutputStreamConsumer;
import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import com.softwareplumbers.feed.FeedExceptions.StreamingException;
import static com.softwareplumbers.feed.FeedExceptions.runtime;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.FeedService;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.impl.MessageImpl;
import com.softwareplumbers.feed.impl.buffer.MessageBuffer;
import com.softwareplumbers.feed.impl.buffer.MessageClock;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.json.Json;
import javax.json.JsonObject;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jonathan
 */
public class TestUtils {
    
    private static Logger LOG = LoggerFactory.getLogger(TestUtils.class);
    
    public static String asString(InputStream is) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            OutputStreamConsumer.of(()->is).consume(stream);
            return stream.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
            
    }

    private static final String[] WORDS = new String[] { "sphagetti", "idle", "loves", "jane", "dog", "hair", "tantric", "slightly", "worm", "likely", "moves", "gets", "fast" };
    
    private static final FeedPath[] FEEDS = new FeedPath[] {
        FeedPath.valueOf("accounts/teamA/baseball"),
        FeedPath.valueOf("accounts/teamA/audit"),
        FeedPath.valueOf("marketing/poker"),
        FeedPath.valueOf("sales/ringthebell")  
    };
    
    private static String randomText(int count) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            builder.append(WORDS[(int)(Math.random() * WORDS.length)]);
            builder.append(" ");
        }
        builder.append((int)(Math.random() * 100));
        return builder.toString();
    } 
    
    public static FeedPath randomFeedPath() {
        return FEEDS[(int)(Math.random() * FEEDS.length)];
    }
    
    public static String randomId() {
        return UUID.randomUUID().toString();
    }
    
    public static FeedPath randomMessagePath() {
        return randomFeedPath().addId(randomId());
    }
    
    public static final MessageClock CLOCK = new MessageClock();
    
    public static Message generateMessage(FeedPath feed) {
        JsonObject testHeaders = Json.createObjectBuilder()
            .add("field1", randomText(1))
            .add("field2", randomText(1)).build();
        InputStream testData = new ByteArrayInputStream(randomText(10).getBytes());
        Instant time = Instant.now(CLOCK);
        FeedPath id = feed.addId(UUID.randomUUID().toString());
        return new MessageImpl(id, "testuser", time, UUID.randomUUID(), testHeaders, testData, -1, false);
    }
    
    public static int getAverageMessageSize() throws IOException {
        byte[] buffer = new byte[10000];
        int pos = 0;
        for (int i = 0; i < 20; i ++) {
            Message message = generateMessage(randomFeedPath());
            pos+=message.getLength();
            pos+=message.getHeaderStream().skip(Long.MAX_VALUE);
        }
        return pos/20;    
    }
    
    public static void randomPause(int maxPause) {
        int pause = (int)(Math.random() * maxPause);
        if (pause > 0) {
            try {
                Thread.sleep(pause); // Make sure messages have different timestamps
            } catch (InterruptedException e) {
                // don't care
            }
        }
    }
    
    public static NavigableMap<FeedPath,Message> generateMessages(int count, int maxPause, FeedPath path, Function<Message, Message> messageConsumer) {
        NavigableMap<FeedPath,Message> result = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            Message message = generateMessage(path);
            result.put(message.getName(), messageConsumer.apply(message));
            randomPause(maxPause);
        }
        return result;
    }
    
    public static NavigableMap<FeedPath,Message> generateMessages(int threads, int count, int maxPause, List<FeedPath> feeds, Function<Message, Message> messageConsumer) {
        ConcurrentSkipListMap<FeedPath,Message> result = new ConcurrentSkipListMap<>();
        for (int i = 0; i < threads; ) {
            for (int j = 0; j < feeds.size() && i < threads; j++, i++) {
                FeedPath feed = feeds.get(j);
                new Thread(() -> {
                    for (int k = 0; k < count; k++) {
                        Message message = generateMessage(feed);
                        result.put(message.getName(), messageConsumer.apply(message));
                        randomPause(maxPause);
                    }
                }).start();
            }
        }
        return result;
    }  
    
    public static Map<FeedPath,Message> getMessagesForFeed(FeedPath path, NavigableMap<FeedPath,Message> messages) {
        return messages.values().stream()
            .filter(msg->msg.getName().startsWith(path))
            .collect(Collectors.toMap(Message::getName, Function.identity()));
    }
   
    
    public static List<FeedPath> getFeeds() {
        return Collections.unmodifiableList(Arrays.asList(FEEDS));
    }
    
    public static void createReceiver(int id, FeedService service, int count, FeedPath path, Instant from, Map<FeedPath,Message> results) {
        try {
            while (count > 0) {
                MessageIterator messages = service.listen(path, from).get();
                Message current = null;
                while (messages.hasNext()) {
                    current = messages.next();
                    results.put(current.getName(), current);
                    LOG.debug("receiver {} munched: {} - {}", id, current.getName(), current.getTimestamp());
                    count--;                
                }
                LOG.debug("receiver {} end batch", id);
                if (current != null) from = current.getTimestamp();
            }
            LOG.debug("receiver {} complete", id);
        } catch (InterruptedException | InvalidPath | ExecutionException e) {
            LOG.error("Error in receiver ", e);
        }
    }
    
    public static void createReceiver(int id, MessageBuffer buffer, int count, Instant from, Map<FeedPath,Message> results) {
        try {
            while (count > 0) {
                MessageIterator messages = buffer.getFutureMessagesAfter(from).get();
                Message current = null;
                while (messages.hasNext()) {
                    current = messages.next();
                    results.put(current.getName(), current);
                    LOG.debug("receiver {} munched: {} - {}", id, current.getName(), current.getTimestamp());
                    count--;                
                }
                if (current != null) from = current.getTimestamp();
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error in receiver ", e);
        }
    }
    
    public static Set<FeedPath> getDifference(Map<FeedPath,Message> sent, Map<FeedPath,Message> received) {
        Set<FeedPath> sentButNotReceived = new TreeSet<>(sent.keySet());
        sentButNotReceived.removeAll(received.keySet());      
        return sentButNotReceived;
    }

    public static int compare(Message a, Message b) {
        int result = a.getFeedName().compareTo(b.getFeedName());
        if (result != 0) return result;
        result = a.getHeaders().toString().compareTo(b.getHeaders().toString());
        if (result != 0) return result;
        result = asString(a.getData()).compareTo(asString(b.getData()));
        return result;
    }
    
    public static Set<FeedPath> getDifferenceIgnoringId(Map<FeedPath,Message> sent, Map<FeedPath,Message> received) {
        Map<Message, FeedPath> sentButNotReceived = new TreeMap<>(TestUtils::compare);
        sent.forEach((k,v)->sentButNotReceived.put(v,k));
        received.forEach((k,v)->sentButNotReceived.remove(v));
        return new TreeSet(sentButNotReceived.values());
    }
    
    public static void assertMapsEqual(Map<FeedPath,Message> sent, Map<FeedPath,Message> received) {
        assertThat(getDifference(sent, received), empty());      
    }
    
    public static void showMissing(Map<FeedPath,Message> sent, Map<FeedPath,Message> received) {
        for (FeedPath missing : getDifference(sent, received)) {
            System.out.println(sent.get(missing));
        }
    }
    
    public static List<Map<FeedPath,Message>> createReceivers(CountDownLatch receivers, MessageBuffer buffer, Instant from, int count) {
        List<Map<FeedPath,Message>> results = new ArrayList<>();
        long createCount = receivers.getCount();
        for (int i = 0; i < createCount; i++) {
            Map<FeedPath,Message> resultMap = new ConcurrentSkipListMap<>();
            results.add(resultMap);
            final int id =  i;
            new Thread(()-> {
                TestUtils.createReceiver(id, buffer, count, from, resultMap);
                receivers.countDown();
            }).start();
        }
        return results;
    }  
    
    public static Map<FeedPath,List<Map<FeedPath,Message>>> createReceivers(CountDownLatch receivers, FeedService service, List<FeedPath> feeds, Instant from, int count) {
        Map<FeedPath,List<Map<FeedPath,Message>>> results = new TreeMap<>();
        long createCount = receivers.getCount();
        for (int i = 0; i < createCount; ) {
            for (int j = 0; i < createCount && j < feeds.size(); j++, i++) {
                List<Map<FeedPath,Message>> result = results.computeIfAbsent(feeds.get(j), fm->new ArrayList<>());
                Map<FeedPath,Message> resultMap = new ConcurrentSkipListMap<>();
                result.add(resultMap);
                final int id =  i;
                final int k = j; // FUCK JAVA LAMBDAS
                new Thread(()-> {
                    TestUtils.createReceiver(id, service, count, feeds.get(k), from, resultMap);
                    receivers.countDown();
                }).start();
            }
        }
        return results;
    }  
    
    public static Map<FeedPath, Message> generateBinaryMessageStream(int count, OutputStream bos) {
        return generateMessages(count, 0, randomFeedPath(), msg-> { 
            try {
                msg.writeHeaders(bos);
                msg.writeData(bos);
                return msg;
            } catch (StreamingException e) {
                throw runtime(e);
            }
        });
    }
    
    public static void dumpThreads() {
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            LOG.debug("\"{}\" {} prio={} tid={} {}", 
                thread.getName(),
                (thread.isDaemon() ? "daemon" : ""),
                thread.getPriority(),
                thread.getId(),
                Thread.State.WAITING.equals(thread.getState()) ? "in Object.wait()" : thread.getState().name().toLowerCase()
            );
            LOG.debug("java.lang.Thread.State: {}",
                thread.getState().equals(Thread.State.WAITING) ? "WAITING (on object monitor)" : thread.getState()
            );
            for (StackTraceElement element: thread.getStackTrace())
                LOG.debug("{}", element);
        }
    }
}
