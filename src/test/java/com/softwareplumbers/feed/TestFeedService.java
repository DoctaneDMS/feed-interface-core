/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author jonathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { LocalConfig.class })
public class TestFeedService {

    @Autowired
    FeedService service;
    
    public void post(Message message) {
        try {
            service.post(message.getFeedName(), message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @Test
    public void testMessageRoundtripSingleThread() throws IOException, InvalidPath, InterruptedException {
        FeedPath path = randomFeedPath();
        Instant start = Instant.now();
        Thread.sleep(10);
        NavigableMap<FeedPath, Message> sentMessages = generateMessages(1000, 2, path, this::post); 
        List<FeedPath> feeds = getFeeds();
        service.dumpState();
        assertThat(sentMessages.size(), equalTo(1000));
        TreeMap<FeedPath, Message> responseMessages = new TreeMap<>();
        int count = 0;
        try (MessageIterator messages = service.sync(path, start)) {
            while (messages.hasNext()) {
                Message received = messages.next();
                responseMessages.put(received.getName(), received);
                assertThat(received.getName(), isIn(sentMessages.keySet()));
                Message sent = sentMessages.get(received.getName());
                assertThat(received, equalTo(sent));
                assertThat(asString(received.getData()), equalTo(asString(sent.getData())));
                count++;
            }
        }
        for (FeedPath message : sentMessages.keySet()) {
            if (!responseMessages.containsKey(message))
                System.out.println("missing: " + message);
            //assertThat(message, isIn(responseMessages.keySet()));
        }
        assertThat(count, equalTo(1000));        
    }
    
    @Test
    public void testMessagesForFeed() throws InterruptedException {
        //more of a test-of-test fixture 
        CountDownLatch latch = new CountDownLatch(800);
        NavigableMap<FeedPath, Message> sentMessages = generateMessages(4, 200, 2, getFeeds(), m->latch.countDown());
        assertThat(latch.await(10, TimeUnit.SECONDS), equalTo(true));
        assertThat(sentMessages.size(), equalTo(800));
        assertThat(getMessagesForFeed(getFeeds().get(0), sentMessages).size(), equalTo(200));
        assertThat(getMessagesForFeed(getFeeds().get(1), sentMessages).size(), equalTo(200));
        assertThat(getMessagesForFeed(getFeeds().get(2), sentMessages).size(), equalTo(200));
        assertThat(getMessagesForFeed(getFeeds().get(3), sentMessages).size(), equalTo(200));
        
    }
    
    @Test
    public void testMessageRoundtripMultipleThreads() throws IOException, InvalidPath, InterruptedException {
        
        Instant start = Instant.now();

        // 8 threads each writing 200 messages split across 4 feeds
        NavigableMap<FeedPath, Message> sentMessages = generateMessages(8, 20, 2, getFeeds(), this::post); 
        List<FeedPath> feeds = getFeeds();
        // 8 receivers split across 4 feeds, each should receive all the messages sent to a single feed.
        CountDownLatch receiverCount = new CountDownLatch(8);
        Map<FeedPath, List<Map<FeedPath,Message>>> receivedMessages = createReceivers(receiverCount, service, feeds, start, 40);
        
        if (receiverCount.await(1, TimeUnit.MINUTE)) {
            assertThat(receivedMessages.size(), equalTo(feeds.size()));  

            for (FeedPath feed : feeds) {
                receivedMessages.get(feed).forEach(map->{
                   assertMapsEqual(getMessagesForFeed(feed, sentMessages), map); 
                });
            }            
        } else {
            fail("timed out");
        }
 
    }
}
