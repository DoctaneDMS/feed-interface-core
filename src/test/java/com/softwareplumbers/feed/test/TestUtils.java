/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.test;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.common.pipedstream.OutputStreamConsumer;
import com.softwareplumbers.feed.Feed;
import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import com.softwareplumbers.feed.FeedExceptions.StreamingException;
import static com.softwareplumbers.feed.FeedExceptions.runtime;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.FeedService;
import com.softwareplumbers.feed.Filters;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.MessageType;
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
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.json.Json;
import javax.json.JsonObject;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import javax.json.JsonValue;

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
        return new MessageImpl(MessageType.NONE, id, "testuser", time, Optional.empty(), Optional.empty(), testHeaders, testData, -1, false);
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
    
    public static Stream<Message> generateMessages(int count, int maxPause, FeedPath path, Function<Message, Message> messageConsumer) {
        ArrayList<Message> messages = new ArrayList(count);
        for (int i = 0; i < count; i++) {
            Message message = generateMessage(path);
            messages.add(messageConsumer.apply(message));
            randomPause(maxPause);
        }
        return messages.stream();
    }
    
    public static CompletableFuture<Stream<Message>> generateMessagesAsync(int count, int maxPause, FeedPath path, Function<Message, Message> messageConsumer) {
        return CompletableFuture.supplyAsync(()->generateMessages(count, maxPause, path, messageConsumer));        
    }
    
    public static CompletableFuture<Stream<Message>> generateMessages(int threads, int count, int maxPause, List<FeedPath> feeds, Function<Message, Message> messageConsumer) {
        CompletableFuture<Stream<Message>> result = CompletableFuture.completedFuture(Stream.of());
        for (int i = 0; i < threads; ) {
            for (int j = 0; j < feeds.size() && i < threads; j++, i++) {
                FeedPath feed = feeds.get(j);
                result = result.thenCombine(generateMessagesAsync(count, maxPause, feed, messageConsumer), (a,b)->Stream.concat(a, b));
            }
        }
        return result;
    }  
     
    public static List<FeedPath> getFeeds() {
        return Collections.unmodifiableList(Arrays.asList(FEEDS));
    }
    
    public static Stream<Message> createReceiver(int id, FeedService service, int count, FeedPath path, Instant from) {
        ArrayList<Message> results = new ArrayList<>(count);
        int iterationLimit = count * 2;
        try {
            while (count > 0 && iterationLimit > 0) {
                try (MessageIterator messages = service.listen(path, from, service.getServerId(), 1000L, Filters.NO_ACKS).get(5, TimeUnit.SECONDS)) {
                    Message current = null;
                    while (messages.hasNext()) {
                        current = messages.next();
                        results.add(current);
                        LOG.debug("receiver {} munched: {} - {}", id, current.getName(), current.getTimestamp());
                        count--;                
                    }
                    LOG.debug("receiver {} end batch", id);
                    if (current != null) from = current.getTimestamp();
                }
                iterationLimit--;
            }
            if (iterationLimit == 0) LOG.warn("Iteration limit exceeded");
            LOG.debug("receiver {} complete", id);
        } catch (TimeoutException | InterruptedException | InvalidPath | ExecutionException e) {
            LOG.error("Error in receiver ", e);
        }
        return results.stream();
    }
    
    public static CompletableFuture<Stream<Message>> createReceiverAsync(int id, FeedService service, int count, FeedPath path, Instant from) {
        return CompletableFuture.supplyAsync(()->createReceiver(id, service, count, path, from));
    }
    
    public static void createReceiver(int id, MessageBuffer buffer, int count, Instant from, Map<FeedPath,Message> results) {
        while (count > 0) {
            try (MessageIterator messages = buffer.getMessagesAfter(from, Filters.NO_ACKS)) {
                Message current = null;
                while (messages.hasNext()) {
                    current = messages.next();
                    results.put(current.getName(), current);
                    LOG.debug("receiver {} munched: {} - {}", id, current.getName(), current.getTimestamp());
                    count--;                
                }
                if (current != null) from = current.getTimestamp();
            }
        }
    }
    
    private static String dumpMessage(Message message) {
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            OutputStreamConsumer.of(message::toStream).consume(os);
            return os.toString();
        } catch (IOException e) {
            return message.toString();
        }
    }
        
    public static class Diff {
        public final Set<Message> notReceived = new TreeSet<>(TestUtils::compare);
        public final List<Message> notSent = new ArrayList<>();
        public final List<Message> duplicatesSent = new ArrayList<>();
        public final List<Message> duplicatesReceived = new ArrayList<>();
        public int totalReceived = 0;
        public int totalSent = 0;
        public Instant firstMessageReceivedAt = Instant.MAX;
        public Instant lastMessageReceivedAt = Instant.MIN;
    
        public Diff(Stream<Message> sent, Stream<Message> received) {
            Set<Message> allSent = new TreeSet<>(TestUtils::compare); 
            Set<Message> allReceived = new TreeSet<>(TestUtils::compare); 
            sent.forEach(message-> {
                totalSent++;
                if (allSent.add(message)) 
                    notReceived.add(message);
                else
                    duplicatesSent.add(message);
                
            });
            received.forEach(message->{
                totalReceived++;
                Instant timestamp = message.getTimestamp();
                firstMessageReceivedAt = firstMessageReceivedAt.isAfter(timestamp) ? timestamp : firstMessageReceivedAt;
                lastMessageReceivedAt = lastMessageReceivedAt.isBefore(timestamp) ? timestamp : lastMessageReceivedAt;
                if (allReceived.add(message)) {
                    if (!notReceived.remove(message)) 
                        notSent.add(message);                    
                } else
                    duplicatesReceived.add(message);
            });
        }
        
        public void dump(PrintStream out) {
            out.format("Total received: %d\n", totalReceived);
            out.format("Total sent: %d\n", totalSent);
            out.format("First message received at: %s\n", firstMessageReceivedAt);
            out.format("Last message received at: %s\n", lastMessageReceivedAt);
            out.format("Duplicates received: %d\n", duplicatesReceived.size());
            duplicatesReceived.forEach(message->out.println(dumpMessage(message)));
            out.format("Duplicates sent: %d\n", duplicatesSent.size());
            duplicatesSent.forEach(message->out.println(dumpMessage(message)));
            out.format("Messages not received: %d\n", notReceived.size());
            notReceived.forEach(message->out.println(dumpMessage(message)));
            out.format("Messages not sent: %d\n", notSent.size());
            notSent.forEach(message->out.println(dumpMessage(message)));                 
        }
        
        public boolean isEmpty() {
            return totalReceived == totalSent && duplicatesReceived.size() == 0 && duplicatesSent.size() == 0 && notReceived.size() == 0 && notSent.size() == 0;
        }
    }
    
    public static <T extends Comparable> int compare(Optional<T> a, Optional<T> b) { 
        if (a.isPresent() && b.isPresent()) return a.get().compareTo(b.get());
        if (a.isPresent()) return 1;
        if (b.isPresent()) return -0;
        return 0;
    }
    
    public static int compare(JsonObject a, JsonObject b) {
        TreeSet<String> keys = new TreeSet<>();
        keys.addAll(a.keySet());
        keys.addAll(b.keySet());
        int result = 0;
        Iterator<String> i = keys.iterator();
        while (i.hasNext() && result == 0) {
            String key = i.next();
            result = a.getOrDefault(key, JsonValue.NULL).toString().compareTo(b.getOrDefault(key, JsonValue.NULL).toString());
        }
        return result;
    }

    public static int compare(Message a, Message b) {
        int result = a.getFeedName().compareTo(b.getFeedName());
        if (result != 0) return result;
        result = a.getType().compareTo(b.getType());
        if (result != 0) return result;
        result = compare(a.getHeaders(), b.getHeaders());
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
       
    public static void assertMatch(Stream<Message> sent, Stream<Message> received) {
        Diff difference = new Diff(sent, received);
        if (!difference.isEmpty()) {
            difference.dump(System.err);
            fail("Sent and received messages do not match"); 
        }
    }
    
    public static void assertNoMore(FeedService node, Feed feed, Message last) {
        try (MessageIterator more = feed.search(node, last.getServerId().get(), last.getTimestamp(), Filters.NO_ACKS)) {
            if (more.hasNext()) {
                int count = 0;
                while (more.hasNext()) {
                    System.err.println("Extra message: " + more.next());
                    count++;
                }
                fail(count + " extra messages for " + feed.getName());
            }
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
    
    public static class Receiver {
        public final FeedPath feed;
        public final Stream<Message> messages;
        public Receiver(FeedPath feed, Stream<Message> messages) { this.feed = feed; this.messages = messages; }
    }
    
    public static CompletableFuture<List<Receiver>> createReceivers(int receivers, FeedService service, List<FeedPath> feeds, Instant from, int count) {
        CompletableFuture<List<Receiver>> result = CompletableFuture.completedFuture(new ArrayList<>());
        for (int i = 0; i < receivers; ) {
            for (int j = 0; i < receivers && j < feeds.size(); j++, i++) {
                FeedPath feed = feeds.get(j);
                CompletableFuture<Stream<Message>> receiver = createReceiverAsync(i, service, count, feed, from);
                result = result.thenCombine(receiver, (array,stream)-> { array.add(new Receiver(feed, stream)); return array; });
            }
        }
        return result;
    }  
    
    public static Stream<Message> generateBinaryMessageStream(int count, OutputStream bos) {
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
