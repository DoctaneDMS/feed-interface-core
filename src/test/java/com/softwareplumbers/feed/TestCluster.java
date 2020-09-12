/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.test.TestUtils;
import com.softwareplumbers.feed.test.TestUtils.Receiver;
import static com.softwareplumbers.feed.test.TestUtils.assertMatch;
import static com.softwareplumbers.feed.test.TestUtils.generateMessages;
import static com.softwareplumbers.feed.test.TestUtils.getDifferenceIgnoringId;
import static com.softwareplumbers.feed.test.TestUtils.getFeeds;
import static com.softwareplumbers.feed.test.TestUtils.randomFeedPath;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
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
        generateMessages(1000, 2, path, message->{ Message ack = feedA.post(nodeA, message); return message.setName(ack.getName()).setTimestamp(ack.getTimestamp()).setServerId(ack.getServerId().get()); }).forEach(message->sentMessages.add(message));
        assertThat(sentMessages.size(), equalTo(1000));
        Stream<Message> responseMessages = TestUtils.createReceiver(0, nodeB, 1000, path, start);
        assertThat(responseMessages.count(), equalTo(1000L));        
    }

    @Test
    public void testMessageRoundtripBidirectional() throws IOException, FeedExceptions.InvalidPath, InterruptedException, ExecutionException, TimeoutException {
        FeedPath path = randomFeedPath();
        List<FeedPath> listOfPaths = Collections.singletonList(path);
        Instant start = Instant.now();
        Thread.sleep(10);
        Feed feedA = nodeA.getFeed(path);
        Feed feedB = nodeB.getFeed(path);
        CompletableFuture<Stream<Message>> sentToA = generateMessages(1, 100, 2, listOfPaths, message->feedA.post(nodeA, message));
        CompletableFuture<Stream<Message>> sentToB = generateMessages(1, 100, 2, listOfPaths, message->feedB.post(nodeB, message));
        CompletableFuture<List<Receiver>> responseMessagesA = TestUtils.createReceivers(1, nodeA, listOfPaths, start, 200);
        CompletableFuture<List<Receiver>> responseMessagesB = TestUtils.createReceivers(1, nodeB, listOfPaths, start, 200);
        CompletableFuture.allOf(responseMessagesA, responseMessagesB, sentToA, sentToB).get(10, TimeUnit.SECONDS);
        ArrayList<Message> allSent = new ArrayList<>();
        sentToA.get().forEach(allSent::add);
        sentToB.get().forEach(allSent::add);
        assertThat(allSent.size(), equalTo(200));
        assertMatch(allSent.stream(), responseMessagesA.get().get(0).messages);
        assertMatch(allSent.stream(), responseMessagesB.get().get(0).messages);
    }
}
