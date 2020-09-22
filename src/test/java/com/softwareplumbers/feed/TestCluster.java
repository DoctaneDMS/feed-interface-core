/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.test.TestUtils;
import com.softwareplumbers.feed.test.TestUtils.Receiver;
import static com.softwareplumbers.feed.test.TestUtils.assertMatch;
import static com.softwareplumbers.feed.test.TestUtils.assertNoMore;
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
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isIn;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 * @author jonat
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { LocalConfig.class })
@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
public class TestCluster {
    
    @Autowired @Qualifier(value="testSimpleClusterNodeA")
    protected FeedService nodeA;

    @Autowired @Qualifier(value="testSimpleClusterNodeB")
    protected FeedService nodeB;
    
    @Autowired
    protected Environment env;
    
    public static Message post(FeedService service, Feed feed, Message message) {
        Message ack = feed.post(service, message); 
        return message.setName(ack.getName()).setTimestamp(ack.getTimestamp()).setServerId(ack.getServerId().get()); 
    }
    
    @Test
    public void testMessageRoundtripSingleThread() throws IOException, FeedExceptions.InvalidPath, InterruptedException, TimeoutException {
        FeedPath path = randomFeedPath();
        Instant start = Instant.now();
        Thread.sleep(10);
        Set<Message> sentMessages = new TreeSet<>(TestUtils::compare);
        Feed feedA = nodeA.getFeed(path);
        final int SEND_COUNT = env.getProperty("test.TestCluster.testMessageRoundtripSingleThread.SEND_COUNT", Integer.class);
        generateMessages(SEND_COUNT, 2, path, message->post(nodeA, feedA, message)).forEach(message->sentMessages.add(message));
        assertThat(sentMessages.size(), equalTo(SEND_COUNT));
        Stream<Message> responseMessages = TestUtils.createReceiver(0, nodeB, SEND_COUNT, path, start);
        TestUtils.assertMatch(sentMessages.stream(), responseMessages);
    }
    
    @Test
    public void testMessageRoundtripMonodirectional() throws IOException, FeedExceptions.InvalidPath, InterruptedException, ExecutionException, TimeoutException {
        FeedPath path = randomFeedPath();
        List<FeedPath> listOfPaths = Collections.singletonList(path);
        Instant start = Instant.now();
        Thread.sleep(10);
        Feed feedA = nodeA.getFeed(path);
        Feed feedB = nodeB.getFeed(path);
        final int SEND_COUNT = env.getProperty("test.TestCluster.testMessageRoundtripBidirectional.SEND_COUNT", Integer.class);
        CompletableFuture<Stream<Message>> sentToA = generateMessages(1, SEND_COUNT, 2, listOfPaths, message->post(nodeA, feedA, message));
        CompletableFuture<List<Receiver>> responseMessagesB = TestUtils.createReceivers(1, nodeB, listOfPaths, start, SEND_COUNT);
        CompletableFuture.allOf(responseMessagesB, sentToA).get(20, TimeUnit.SECONDS);
        List<Message> allSent = new ArrayList<>();
        sentToA.get().forEach(allSent::add);
        assertThat(allSent.size(), equalTo(SEND_COUNT));
        List<Message> allReceivedB = responseMessagesB.get().get(0).messages.collect(Collectors.toList());
        assertThat(allReceivedB, hasSize(SEND_COUNT));
        Message lastReceivedB = allReceivedB.get(SEND_COUNT - 1);
        assertMatch(allSent.stream(), allReceivedB.stream());
        Thread.sleep(100);
        assertNoMore(nodeB, feedB, lastReceivedB);
    }    

    @Test
    public void testMessageRoundtripBidirectional() throws IOException, FeedExceptions.InvalidPath, InterruptedException, ExecutionException, TimeoutException {
        FeedPath path = randomFeedPath();
        List<FeedPath> listOfPaths = Collections.singletonList(path);
        Instant start = Instant.now();
        Thread.sleep(10);
        Feed feedA = nodeA.getFeed(path);
        Feed feedB = nodeB.getFeed(path);
        final int SEND_COUNT = env.getProperty("test.TestCluster.testMessageRoundtripBidirectional.SEND_COUNT", Integer.class);
        CompletableFuture<Stream<Message>> sentToA = generateMessages(1, SEND_COUNT, 2, listOfPaths, message->post(nodeA, feedA, message));
        CompletableFuture<Stream<Message>> sentToB = generateMessages(1, SEND_COUNT, 2, listOfPaths, message->post(nodeB, feedB, message));
        CompletableFuture<List<Receiver>> responseMessagesA = TestUtils.createReceivers(1, nodeA, listOfPaths, start, SEND_COUNT * 2);
        CompletableFuture<List<Receiver>> responseMessagesB = TestUtils.createReceivers(1, nodeB, listOfPaths, start, SEND_COUNT * 2);
        CompletableFuture.allOf(responseMessagesA, responseMessagesB, sentToA, sentToB).get(10, TimeUnit.SECONDS);
        ArrayList<Message> allSent = new ArrayList<>();
        sentToA.get().forEach(allSent::add);
        sentToB.get().forEach(allSent::add);
        assertThat(allSent.size(), equalTo(SEND_COUNT * 2));
        List<Message> allReceivedA = responseMessagesA.get().get(0).messages.collect(Collectors.toList());
        List<Message> allReceivedB = responseMessagesB.get().get(0).messages.collect(Collectors.toList());
        assertThat(allReceivedA, hasSize(SEND_COUNT * 2));
        assertThat(allReceivedB, hasSize(SEND_COUNT * 2));
        Message lastReceivedA = allReceivedA.get(SEND_COUNT * 2 - 1);
        Message lastReceivedB = allReceivedB.get(SEND_COUNT * 2 - 1);
        assertMatch(allSent.stream(), allReceivedA.stream());
        assertMatch(allSent.stream(), allReceivedB.stream());
        Thread.sleep(100);
        assertNoMore(nodeA, feedA, lastReceivedA);
        assertNoMore(nodeB, feedB, lastReceivedB);
    }
}
