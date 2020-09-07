/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import static com.softwareplumbers.feed.test.TestUtils.generateMessages;
import static com.softwareplumbers.feed.test.TestUtils.getFeeds;
import static com.softwareplumbers.feed.test.TestUtils.randomFeedPath;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertFalse;
import org.junit.Test;

/**
 *
 * @author jonathan
 */
public class TestMessageIterator {
    
    @Test
    public void testSequence() {
        Map<FeedPath,Message> messages1 = generateMessages(3,2,randomFeedPath(),m->m);
        Map<FeedPath,Message> messages2 = generateMessages(4,2,randomFeedPath(),m->m);
        Map<FeedPath,Message> messages3 = generateMessages(5,2,randomFeedPath(),m->m);
        
        MessageIterator seq = MessageIterator.of(
            MessageIterator.of(messages1.values().iterator(), ()->{}),
            MessageIterator.of(messages2.values().iterator(), ()->{}),
            MessageIterator.of(messages3.values().iterator(), ()->{})
        );
        
        Set<FeedPath> received = new TreeSet<>();
        while (seq.hasNext()) received.add(seq.next().getName());
        
        for (FeedPath message : messages1.keySet()) {
            assertThat(received, hasItem(message));
        }

        for (FeedPath message : messages2.keySet()) {
            assertThat(received, hasItem(message));
        }
        
        for (FeedPath message : messages3.keySet()) {
            assertThat(received, hasItem(message));
        }
    }

    @Test
    public void testPeekable() {
        Map<FeedPath,Message> messages1 = generateMessages(3,2,randomFeedPath(),m->m);
        
        MessageIterator.Peekable seq = MessageIterator.of(messages1.values().iterator(), ()->{}).peekable();
        
        Optional<Message> peeked = seq.peek();
        while (seq.hasNext()) {
            assertThat(seq.next(), equalTo(peeked.get()));
            peeked = seq.peek();
        }
        
        assertFalse(peeked.isPresent());
    }
    
    @Test
    public void testMerge() throws InterruptedException {
        
        final int THREADS = 2;
        final int MESSAGES = 20;
        CountDownLatch receiving = new CountDownLatch(3 * THREADS * MESSAGES);
        
        Collection<Message> messages1 = generateMessages(2,20,2,getFeeds(),m->{ receiving.countDown(); return m; }).values();
        Collection<Message> messages2 = generateMessages(2,20,2,getFeeds(),m->{ receiving.countDown(); return m; }).values();
        Collection<Message> messages3 = generateMessages(2,20,2,getFeeds(),m->{ receiving.countDown(); return m; }).values();
        
        receiving.await(10, TimeUnit.SECONDS);
        
        MessageIterator seq = MessageIterator.merge(
            MessageIterator.of(messages1.stream().sorted(Comparator.comparing(Message::getTimestamp))),
            MessageIterator.of(messages2.stream().sorted(Comparator.comparing(Message::getTimestamp))),
            MessageIterator.of(messages3.stream().sorted(Comparator.comparing(Message::getTimestamp)))
        );
        
        Set<FeedPath> received = new TreeSet<>();
        Instant previous = Instant.MIN;
        
        while (seq.hasNext()) {
            Message message = seq.next();
            // Check that messages are in chronological order
            assertThat(previous, lessThan(message.getTimestamp()));
            previous = message.getTimestamp();
            received.add(message.getName());
        }
        
        for (Message message : messages1) {
            assertThat(received, hasItem(message.getName()));
        }

        for (Message message : messages2) {
            assertThat(received, hasItem(message.getName()));
        }
        
        for (Message message : messages3) {
            assertThat(received, hasItem(message.getName()));
        }        
    }
}
