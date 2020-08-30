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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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
            return service.post(message.getFeedName(), message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @Test
    public void testMessageRoundtripSingleThread() throws IOException, InvalidPath, InterruptedException {
        FeedPath path = randomFeedPath();
        Instant start = Instant.now();
        Thread.sleep(10);
        Set<Message> sentMessages = new TreeSet<>(TestUtils::compare);
        generateMessages(1000, 2, path, this::post).forEach((k,v)->sentMessages.add(v));
        List<FeedPath> feeds = getFeeds();
        //service.dumpState();
        assertThat(sentMessages.size(), equalTo(1000));
        TreeMap<FeedPath, Message> responseMessages = new TreeMap<>();
        int count = 0;
        try (MessageIterator messages = service.sync(path, start, service.getServerId())) {
            while (messages.hasNext()) {
                Message received = messages.next();
                responseMessages.put(received.getName(), received);
                assertThat(received, isIn(sentMessages));
                count++;
            }
        }
        assertThat(count, equalTo(1000));        
    }
    
    @Test 
    public void testCancelCallback() throws InvalidPath, InterruptedException {
        
        BlockingQueue<Message> results = new ArrayBlockingQueue<Message>(10); 
        
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
        service.listen(feed, Instant.now(), callback);
        service.cancelCallback(feed, callback);
        post(generateMessage(feed));
               
        assertThat(results.poll(100, TimeUnit.MILLISECONDS), nullValue());
    }
    
    @Test
    public void testMessagesForFeed() throws InterruptedException {
        //more of a test-of-test fixture 
        CountDownLatch latch = new CountDownLatch(800);
        NavigableMap<FeedPath, Message> sentMessages = generateMessages(4, 200, 2, getFeeds(), m->{ latch.countDown(); return m; });
        assertThat(latch.await(20, TimeUnit.SECONDS), equalTo(true));
         // necessary because the latch countdown happens before the message is added to the send message map.
         // this isn't the idea solution but it's only test code.
        Thread.sleep(10);
        assertThat(sentMessages.size(), equalTo(800));
        assertThat(getMessagesForFeed(getFeeds().get(0), sentMessages).size(), equalTo(200));
        assertThat(getMessagesForFeed(getFeeds().get(1), sentMessages).size(), equalTo(200));
        assertThat(getMessagesForFeed(getFeeds().get(2), sentMessages).size(), equalTo(200));
        assertThat(getMessagesForFeed(getFeeds().get(3), sentMessages).size(), equalTo(200));
        
    }
    
    @Test
    public void testMessageRoundtripMultipleThreads() throws IOException, InvalidPath, InterruptedException {
        
        Instant start = Instant.now();

        // 8 threads each writing 20 messages split across 4 feeds
        NavigableMap<FeedPath, Message> sentMessages = generateMessages(8, 20, 2, getFeeds(), this::post); 
        List<FeedPath> feeds = getFeeds();
        // 8 receivers split across 4 feeds, each should receive all the messages sent to a single feed.
        CountDownLatch receiverCount = new CountDownLatch(8);
        Map<FeedPath, List<Map<FeedPath,Message>>> receivedMessages = createReceivers(receiverCount, service, feeds, start, 40);
        
        if (receiverCount.await(60, TimeUnit.SECONDS)) {
            assertThat(receivedMessages.size(), equalTo(feeds.size()));  

            for (FeedPath feed : feeds) {
                receivedMessages.get(feed).forEach(map->{
                   Set<FeedPath> dropped = getDifferenceIgnoringId(getMessagesForFeed(feed, sentMessages), map);
                   assertThat(dropped, hasSize(0));
                });
            }            
        } else {
            System.out.println("receiverCount " + receiverCount.getCount());
            for (FeedPath feed : receivedMessages.keySet()) {
                dumpThreads();
                List<Map<FeedPath,Message>> receivers = receivedMessages.get(feed);
                for (int i = 0; i < receivers.size(); i++) {
                    Map<FeedPath, Message> map = receivers.get(i);
                    System.out.println("receiver " + i + " has " + map.size() + " messages for " + feed);
                    Map<FeedPath, Message> sent = getMessagesForFeed(feed, sentMessages);
                    Set<FeedPath> dropped = getDifferenceIgnoringId(sent, map);
                    for (FeedPath path : dropped) {
                        System.out.println("lost message " + path + "[" + sent.get(path) + "]");
                    }
                }
            }
            fail("timed out");
        }
 
    }
}
