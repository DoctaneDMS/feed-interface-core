/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.test.TestUtils;
import static com.softwareplumbers.feed.test.TestUtils.generateMessages;
import static com.softwareplumbers.feed.test.TestUtils.getDifferenceIgnoringId;
import static com.softwareplumbers.feed.test.TestUtils.getFeeds;
import static com.softwareplumbers.feed.test.TestUtils.getMessagesForFeed;
import static com.softwareplumbers.feed.test.TestUtils.randomFeedPath;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isIn;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 * @author jonat
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { LocalConfig.class })
public class TestCluster {
    
    @Autowired @Qualifier(value="testSimpleClusterNodeA")
    protected FeedService nodeA;

    @Autowired @Qualifier(value="testSimpleClusterNodeB")
    protected FeedService nodeB;
    
    @Test
    public void testMessageRoundtripSingleThread() throws IOException, FeedExceptions.InvalidPath, InterruptedException, TimeoutException {
        FeedPath path = randomFeedPath();
        Instant start = Instant.now();
        Thread.sleep(10);
        Set<Message> sentMessages = new TreeSet<>(TestUtils::compare);
        Feed feedA = nodeA.getFeed(path);
        generateMessages(1000, 2, path, message->feedA.post(nodeA, message)).forEach((k,v)->sentMessages.add(v));
        assertThat(sentMessages.size(), equalTo(1000));
        TreeMap<FeedPath, Message> responseMessages = new TreeMap<>();
        TestUtils.createReceiver(0, nodeB, 1000, path, start, responseMessages);
        assertThat(responseMessages.size(), equalTo(1000));        
    }

    @Test
    public void testMessageRoundtripBidirectional() throws IOException, FeedExceptions.InvalidPath, InterruptedException {
        FeedPath path = randomFeedPath();
        List<FeedPath> listOfPaths = Collections.singletonList(path);
        Instant start = Instant.now();
        Thread.sleep(10);
        Feed feedA = nodeA.getFeed(path);
        Feed feedB = nodeB.getFeed(path);
        NavigableMap<FeedPath, Message> sentToA = generateMessages(1, 500, 2, listOfPaths, message->feedA.post(nodeA, message));
        NavigableMap<FeedPath, Message> sentToB = generateMessages(1, 500, 2, listOfPaths, message->feedB.post(nodeB, message));
        CountDownLatch aDone = new CountDownLatch(1);
        CountDownLatch bDone = new CountDownLatch(1);
        Map<FeedPath,List<Map<FeedPath,Message>>> responseMessagesA = TestUtils.createReceivers(aDone, nodeA, listOfPaths, start, 1000);
        Map<FeedPath,List<Map<FeedPath,Message>>> responseMessagesB = TestUtils.createReceivers(bDone, nodeB, listOfPaths, start, 1000);
        aDone.await();
        bDone.await();
        NavigableMap<FeedPath, Message> allSent = new TreeMap<>();
        allSent.putAll(sentToA);
        allSent.putAll(sentToB);
        assertThat(allSent.size(), equalTo(1000));
        for (FeedPath feed : listOfPaths) {
            responseMessagesB.get(feed).forEach(map->{
               Set<FeedPath> dropped = getDifferenceIgnoringId(getMessagesForFeed(feed, allSent), map);
               assertThat(dropped, hasSize(0));
            });
            responseMessagesA.get(feed).forEach(map->{
               Set<FeedPath> dropped = getDifferenceIgnoringId(getMessagesForFeed(feed, allSent), map);
               assertThat(dropped, hasSize(0));
            });
        }        
    }
}
