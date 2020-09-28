
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.json.JsonValue;
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
        Map<FeedPath,Message> messages1 = generateMessages(3,2,randomFeedPath(),m->m).collect(Collectors.toMap(m->m.getName(), m->m));
        Map<FeedPath,Message> messages2 = generateMessages(4,2,randomFeedPath(),m->m).collect(Collectors.toMap(m->m.getName(), m->m));
        Map<FeedPath,Message> messages3 = generateMessages(5,2,randomFeedPath(),m->m).collect(Collectors.toMap(m->m.getName(), m->m));
        
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
        Map<FeedPath,Message> messages1 = generateMessages(3,2,randomFeedPath(),m->m).collect(Collectors.toMap(m->m.getName(), m->m));
        
        MessageIterator.Peekable seq = MessageIterator.of(messages1.values().iterator(), ()->{}).peekable();
        
        Optional<Message> peeked = seq.peek();
        while (seq.hasNext()) {
            assertThat(seq.next(), equalTo(peeked.get()));
            peeked = seq.peek();
        }
        
        assertFalse(peeked.isPresent());
    }
    
    @Test
    public void testFilter() {
        Map<FeedPath,Message> messages1 = generateMessages(3,2,randomFeedPath(),m->m).collect(Collectors.toMap(m->m.getName(), m->m));
        
        final long averageLength = messages1.values().stream().map(Message::getLength).reduce(0L, (a,b)->a+b) / messages1.size();
        
        MessageIterator filter = MessageIterator.of(messages1.values().iterator(), ()->{}).filter(message->message.getLength() < averageLength);
        Iterator<Message> reference = messages1.values().stream().filter(message->message.getLength() < averageLength).iterator();
                
        while (filter.hasNext() && reference.hasNext()) {
            assertThat(filter.next(), equalTo(reference.next()));
        }
        
        assertFalse(filter.hasNext());
        assertFalse(reference.hasNext());
    }
    
    @Test
    public void testMerge() throws InterruptedException, ExecutionException, TimeoutException {
        
        final int THREADS = 2;
        final int MESSAGES = 20;
        
        CompletableFuture<List<Message>> messages1 = generateMessages(2, 20, 2, getFeeds(),m->m)
            .thenApply(stream->stream.sorted(Comparator.comparing(Message::getTimestamp)).collect(Collectors.toList()));
        CompletableFuture<List<Message>> messages2 = generateMessages(2, 20, 2, getFeeds(),m->m)
            .thenApply(stream->stream.sorted(Comparator.comparing(Message::getTimestamp)).collect(Collectors.toList()));
        CompletableFuture<List<Message>> messages3 = generateMessages(2, 20, 2, getFeeds(),m->m)
            .thenApply(stream->stream.sorted(Comparator.comparing(Message::getTimestamp)).collect(Collectors.toList()));
        
        MessageIterator seq = MessageIterator.merge(
            MessageIterator.of(messages1.get(10, TimeUnit.SECONDS).stream()),
            MessageIterator.of(messages2.get(10, TimeUnit.SECONDS).stream()),
            MessageIterator.of(messages3.get(10, TimeUnit.SECONDS).stream())
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
        
        for (Message message : messages1.get()) {
            assertThat(received, hasItem(message.getName()));
        }

        for (Message message : messages2.get()) {
            assertThat(received, hasItem(message.getName()));
        }
        
        for (Message message : messages3.get()) {
            assertThat(received, hasItem(message.getName()));
        }        
    }
}
