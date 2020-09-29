/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidState;
import com.softwareplumbers.feed.test.TestUtils;
import com.softwareplumbers.feed.test.TestUtils.Receiver;
import static com.softwareplumbers.feed.test.TestUtils.assertMatch;
import static com.softwareplumbers.feed.test.TestUtils.assertNoMore;
import static com.softwareplumbers.feed.test.TestUtils.generateMessages;
import static com.softwareplumbers.feed.test.TestUtils.randomFeedPath;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;
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
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(TestCluster.class);
    
    @Autowired @Qualifier(value="testSimpleClusterNodeA")
    protected FeedService nodeA;

    @Autowired @Qualifier(value="testSimpleClusterNodeB")
    protected FeedService nodeB;

    @Autowired @Qualifier(value="testSimpleClusterNodeB")
    protected FeedService nodeC;

    @Autowired @Qualifier(value="testSimpleCluster")
    protected Cluster cluster;

    @Autowired @Qualifier(value="remoteSimpleCluster")
    protected Cluster remote;
    
    @Autowired
    protected Environment env;
    
    public static Message post(FeedService service, Feed feed, Message message) {
        try {
            Message ack = feed.post(service, message); 
            return message.setName(ack.getName()).setTimestamp(ack.getTimestamp()).setServerId(ack.getServerId().get()); 
        } catch (InvalidState ex) {
            throw new RuntimeException(ex);
        }
    }
    
    protected long getTimeout() {
        Long value = env.getProperty("test.TestCluster.TIMEOUT", Long.class);
        return value == null ? 20 : value;
    }

    @Test
    public void testMessageRoundtripSingleThread() throws IOException, FeedExceptions.InvalidPath, InterruptedException, TimeoutException {
        FeedPath path = randomFeedPath();
        Instant start = nodeB.getLastTimestamp(FeedPath.ROOT).orElseGet(()->nodeB.getInitTime());
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
        LOG.entry();
        FeedPath path = randomFeedPath();
        List<FeedPath> listOfPaths = Collections.singletonList(path);
        Instant startB = nodeB.getLastTimestamp(FeedPath.ROOT).orElseGet(()->nodeB.getInitTime());
        Instant startC = nodeC.getLastTimestamp(FeedPath.ROOT).orElseGet(()->nodeC.getInitTime());
        Feed feedA = nodeA.getFeed(path);
        Feed feedB = nodeB.getFeed(path);
        Feed feedC = nodeC.getFeed(path);
        final int SEND_COUNT = env.getProperty("test.TestCluster.testMessageRoundtripMonodirectional.SEND_COUNT", Integer.class);
        LOG.info("Creating sender thread");
        CompletableFuture<Stream<Message>> sentToA = generateMessages(1, SEND_COUNT, 2, listOfPaths, message->post(nodeA, feedA, message));
        LOG.info("Creating receiver threads");
        CompletableFuture<List<Receiver>> responseMessagesB = TestUtils.createReceivers(1, nodeB, listOfPaths, startB, SEND_COUNT);
        CompletableFuture<List<Receiver>> responseMessagesC = TestUtils.createReceivers(1, nodeC, listOfPaths, startC, SEND_COUNT);
        CompletableFuture.allOf(responseMessagesB, responseMessagesC, sentToA).get(getTimeout(), TimeUnit.SECONDS);
        LOG.info("sender/receiver threads complete");
        List<Message> allSent = new ArrayList<>();
        sentToA.get().forEach(allSent::add);
        assertThat(allSent.size(), equalTo(SEND_COUNT));
        List<Message> allReceivedB = responseMessagesB.get().get(0).messages.collect(Collectors.toList());
        assertThat(allReceivedB, hasSize(SEND_COUNT));
        Message lastReceivedB = allReceivedB.get(SEND_COUNT - 1);
        assertMatch(allSent.stream(), allReceivedB.stream());
        List<Message> allReceivedC = responseMessagesC.get().get(0).messages.collect(Collectors.toList());
        assertThat(allReceivedC, hasSize(SEND_COUNT));
        Message lastReceivedC = allReceivedC.get(SEND_COUNT - 1);
        assertMatch(allSent.stream(), allReceivedB.stream());
        Thread.sleep(100);
        assertNoMore(nodeB, feedB, lastReceivedB);
        assertNoMore(nodeC, feedC, lastReceivedC);
        LOG.exit();
    }    

    @Test
    public void testMessageRoundtripBidirectional() throws IOException, FeedExceptions.InvalidPath, InterruptedException, ExecutionException, TimeoutException {
        LOG.entry();
        FeedPath path = randomFeedPath();
        List<FeedPath> listOfPaths = Collections.singletonList(path);
        Instant startA = nodeA.getLastTimestamp(FeedPath.ROOT).orElseGet(()->nodeA.getInitTime());
        Instant startB = nodeB.getLastTimestamp(FeedPath.ROOT).orElseGet(()->nodeB.getInitTime());
        Instant startC = nodeC.getLastTimestamp(FeedPath.ROOT).orElseGet(()->nodeC.getInitTime());
        Feed feedA = nodeA.getFeed(path);
        Feed feedB = nodeB.getFeed(path);
        final int SEND_COUNT = env.getProperty("test.TestCluster.testMessageRoundtripBidirectional.SEND_COUNT", Integer.class);
        LOG.info("Creating sender threads");
        CompletableFuture<Stream<Message>> sentToA = generateMessages(1, SEND_COUNT, 2, listOfPaths, message->post(nodeA, feedA, message));
        CompletableFuture<Stream<Message>> sentToB = generateMessages(1, SEND_COUNT, 2, listOfPaths, message->post(nodeB, feedB, message));
        LOG.info("Creating receiver threads");
        CompletableFuture<List<Receiver>> responseMessagesA = TestUtils.createReceivers(1, nodeA, listOfPaths, startA, SEND_COUNT * 2);
        CompletableFuture<List<Receiver>> responseMessagesB = TestUtils.createReceivers(1, nodeB, listOfPaths, startB, SEND_COUNT * 2);
        CompletableFuture<List<Receiver>> responseMessagesC = TestUtils.createReceivers(1, nodeC, listOfPaths, startC, SEND_COUNT * 2);
        CompletableFuture.allOf(responseMessagesA, responseMessagesB, responseMessagesC, sentToA, sentToB).get(getTimeout(), TimeUnit.SECONDS);
        LOG.info("sender/receiver threads complete");
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
        LOG.exit();
    }
    
    @Test 
    public void testDumpState() throws InterruptedException {
        System.out.println("testDumpState");
        try (PrintWriter writer = new PrintWriter(System.out)) {
            writer.println("'local' cluster");
            cluster.dumpState(writer);
            writer.println("'remote' cluster");
            remote.dumpState(writer);
        }
    }
}
