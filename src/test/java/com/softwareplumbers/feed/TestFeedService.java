/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidId;
import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import com.softwareplumbers.feed.FeedExceptions.InvalidState;
import com.softwareplumbers.feed.test.TestUtils;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import static com.softwareplumbers.feed.test.TestUtils.*;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;
import static org.mockito.Matchers.notNull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.test.annotation.DirtiesContext;

/**
 *
 * @author jonathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { LocalConfig.class })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class TestFeedService {

    @Autowired @Qualifier(value="testService")
    protected FeedService service;
    
    @Autowired
    protected Environment env;
    
    public Message post(Message message) {
        try {
            Message ack = service.post(message.getFeedName(), message);
            // This should be the same as the message ultimately received *on the same server as it was posted*
            return TestUtils.expectedReturn(message, ack);
        } catch (InvalidPath | InvalidState e) {
            throw new RuntimeException(e);
        }
    }
    
    protected long getTimeout() {
        Long value = env.getProperty("test.TestFeedService.TIMEOUT", Long.class);
        return value == null ? 20 : value;
    }    
    
    @Test
    public void testMessageRoundtripSingleThread() throws IOException, InvalidPath, InterruptedException {
        FeedPath path = randomFeedPath();
        Instant start = service.getLastTimestamp(path).orElseGet(()->service.getInitTime());
        Set<Message> sentMessages = new TreeSet<>(TestUtils::compare);
        final int SEND_COUNT = env.getProperty("test.TestFeedService.testMessageRoundtripSingleThread.SEND_COUNT", Integer.class);        
        generateMessages(SEND_COUNT, 2, path, this::post).forEach(message->sentMessages.add(message));
        List<FeedPath> feeds = getFeeds();
        //service.dumpState();
        assertThat(sentMessages.size(), equalTo(SEND_COUNT));
        TreeMap<FeedPath, Message> responseMessages = new TreeMap<>();
        int count = 0;
        try (MessageIterator messages = service.search(path, service.getServerId(), start, Filters.NO_ACKS)) {
            while (messages.hasNext()) {
                Message received = messages.next();
                responseMessages.put(received.getName(), received);
                //received = received.setName(received.getName()); 
                assertThat(received, isIn(sentMessages));
                count++;
            }
        }
        assertThat(count, equalTo(SEND_COUNT));        
    }
    
    @Test
    public void testSearchByMessageId() throws IOException, InvalidPath, InvalidId, InterruptedException {
        FeedPath path = randomFeedPath();
        NavigableSet<Message> sentMessages = new TreeSet<>(TestUtils::compare);
        generateMessages(1, 0, path, this::post).forEach(message->sentMessages.add(message));
        assertThat(sentMessages.size(), equalTo(1));
        try (MessageIterator messages = service.search(sentMessages.first().getName(), Filters.NO_ACKS)) {
            assertTrue(messages.hasNext());
            assertThat(messages.next(), isIn(sentMessages));
        }
    }    
    
    @Test
    public void testServiceInfo() {
        assertThat(service.getServerId(), notNullValue());
        assertThat(service.getInitTime(), notNullValue());
    }   

    @Test
    public void testGetFeedInfo() throws InvalidPath {
        FeedPath path = randomFeedPath();
        Message expectedReturn = post(generateMessage(path));
        Feed feed = service.getFeed(path);
        assertThat(feed.getName(), equalTo(path));
        assertThat(feed.getLastTimestamp().get(), equalTo(expectedReturn.getTimestamp()));
        assertThat(feed.getLastTimestamp(service).get(), equalTo(expectedReturn.getTimestamp()));
        assertThat(service.getLastTimestamp(path).get(), equalTo(expectedReturn.getTimestamp()));
    } 

    @Test
    public void testGetFeedChildren() throws InvalidPath {
        FeedPath path = randomFeedPath();
        FeedPath childA = path.add("a");
        FeedPath childB = path.add("b");
        FeedPath grandChild = childA.add("c");
        
        post(generateMessage(childA));
        post(generateMessage(childB));
        post(generateMessage(grandChild));
        
        assertThat(service.getChildren(path).count(), equalTo(2L));
        assertThat(service.getFeed(path).getChildren(service).count(), equalTo(2L));
        
        assertThat(service.getChildren(childA).count(), equalTo(1L));
        assertThat(service.getFeed(childA).getChildren(service).count(), equalTo(1L));

        assertThat(service.getChildren(childB).count(), equalTo(0L));
        assertThat(service.getFeed(childB).getChildren(service).count(), equalTo(0L));

        service.getChildren(path).forEach(child->{
            assertThat(child.getName(), anyOf(equalTo(childA), equalTo(childB)));
        });
    } 

    
    @Test 
    public void testCancelCallback() throws InvalidPath, InterruptedException {
        
        BlockingQueue<Message> results = new ArrayBlockingQueue<>(10); 
        
        Consumer<MessageIterator> callback = mi->{
            mi.forEachRemaining(m->{
                try {
                    results.put(m);
                } catch (InterruptedException e) {
                    // hate these
                }
            });
        };
          
        FeedPath feed = randomFeedPath();
        CompletableFuture<MessageIterator> result = service.listen(feed, Instant.now(), service.getServerId(), 10000L);
        result.thenAccept(callback);
        result.cancel(true);
        post(generateMessage(feed));
               
        assertThat(results.poll(100, TimeUnit.MILLISECONDS), nullValue());
    }
    
    @Test
    public void testMessagesForFeed() throws InterruptedException, ExecutionException, TimeoutException {
        //more of a test-of-test fixture 
        List<Message> sentMessages = generateMessages(4, 200, 2, getFeeds(), m->m).get(getTimeout(), TimeUnit.SECONDS).collect(Collectors.toList());
         // necessary because the latch countdown happens before the message is added to the send message map.
         // this isn't the idea solution but it's only test code.
        assertThat(sentMessages.size(), equalTo(800));
        assertThat(sentMessages.stream().filter(message->message.getFeedName().equals(getFeeds().get(0))).count(), equalTo(200L));
        assertThat(sentMessages.stream().filter(message->message.getFeedName().equals(getFeeds().get(1))).count(), equalTo(200L));
        assertThat(sentMessages.stream().filter(message->message.getFeedName().equals(getFeeds().get(2))).count(), equalTo(200L));
        assertThat(sentMessages.stream().filter(message->message.getFeedName().equals(getFeeds().get(3))).count(), equalTo(200L));
        
    }
    
    @Test
    public void testMessageRoundtripMultipleThreads() throws IOException, InvalidPath, InterruptedException, ExecutionException {
        
        Instant start = service.getLastTimestamp(FeedPath.ROOT).orElseGet(()->service.getInitTime());

        final int SEND_COUNT = env.getProperty("test.TestFeedService.testMessageRoundtripMultipleThreads.SEND_COUNT", Integer.class);   
        final int THREADS = 8;
        
        // 8 threads each writing 20 messages split across 4 feeds
        CompletableFuture<Stream<Message>> sentMessages = generateMessages(THREADS, SEND_COUNT, 2, getFeeds(), this::post); 
        List<FeedPath> feeds = getFeeds();
        // 8 receivers split across 4 feeds, each should receive all the messages sent to a single feed.
        try {
            List<Receiver> receivers = createReceivers(THREADS, service, feeds, start, SEND_COUNT * THREADS / feeds.size()).get(getTimeout(), TimeUnit.SECONDS);

            assertThat(receivers.size(), equalTo(THREADS));  
            
            List<Message> sentList = sentMessages.get().collect(Collectors.toList());

            for (Receiver results : receivers) {
                TestUtils.assertMatch(sentList.stream().filter(m->m.getFeedName().equals(results.feed)), results.messages);
            }            
        } catch(TimeoutException exp) {
            dumpThreads();
            fail("timed out");
        } 
    }
    
    
    @Test
    public void testListenerTimeout() throws IOException, InvalidPath, InterruptedException, ExecutionException, TimeoutException {
        FeedPath path = randomFeedPath();
        Instant start = Instant.now();
        Thread.sleep(10);
        try (MessageIterator messages = service.listen(path, Instant.now(), service.getServerId(), 1000).get(5000, TimeUnit.MILLISECONDS)) {
            // Basically this should time out after 1000 milliseconds rather than waiting for the future to timeout at 5000 milliseconds.
            // we should get an empty iterator rather than a TimeoutException
            assertThat(messages.hasNext(), equalTo(false));
        }
    }    
}
