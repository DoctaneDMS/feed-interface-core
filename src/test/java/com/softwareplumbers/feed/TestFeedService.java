/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
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
import static org.junit.Assert.fail;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 *
 * @author jonathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { LocalConfig.class })
public class TestFeedService {

    @Autowired @Qualifier(value="testService")
    protected FeedService service;
    
    public Message post(Message message) {
        try {
            Message ack = service.post(message.getFeedName(), message);
            // This should be the same as the message ultimately received *on the same server as it was posted*
            return message.setName(ack.getName()).setTimestamp(ack.getTimestamp()).setServerId(ack.getServerId().get());
        } catch (InvalidPath e) {
            throw new RuntimeException(e);
        }
    }
    
    @Test
    public void testMessageRoundtripSingleThread() throws IOException, InvalidPath, InterruptedException {
        FeedPath path = randomFeedPath();
        Instant start = Instant.now();
        Thread.sleep(10);
        Set<Message> sentMessages = new TreeSet<>(TestUtils::compare);
        generateMessages(1000, 2, path, this::post).forEach(message->sentMessages.add(message));
        List<FeedPath> feeds = getFeeds();
        //service.dumpState();
        assertThat(sentMessages.size(), equalTo(1000));
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
        assertThat(count, equalTo(1000));        
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
        List<Message> sentMessages = generateMessages(4, 200, 2, getFeeds(), m->m).get(10, TimeUnit.SECONDS).collect(Collectors.toList());
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
        
        Instant start = Instant.now();

        // 8 threads each writing 20 messages split across 4 feeds
        CompletableFuture<Stream<Message>> sentMessages = generateMessages(8, 20, 2, getFeeds(), this::post); 
        List<FeedPath> feeds = getFeeds();
        // 8 receivers split across 4 feeds, each should receive all the messages sent to a single feed.
        try {
            List<Receiver> receivers = createReceivers(8, service, feeds, start, 40).get(60, TimeUnit.SECONDS);

            assertThat(receivers.size(), equalTo(8));  
            
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
