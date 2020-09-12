/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import com.softwareplumbers.feed.FeedExceptions.StreamingException;
import static com.softwareplumbers.feed.FeedExceptions.runtime;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.Message;
import java.io.PrintWriter;
import java.time.Clock;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/**
 *
 * @author jonathan
 */
public class MessageBuffer {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(BufferPool.class);
    
    private final NavigableMap<Instant, Bucket> bucketCache = new ConcurrentSkipListMap<>();
    private Bucket current;
    private final BufferPool pool;
    private final Clock clock;
    
    MessageBuffer(BufferPool pool, MessageClock clock, int initSize) {
        this.pool = pool;
        this.clock = clock;
        allocateNewBucket(initSize, Instant.now());
    }
    
    protected int calcNewSize(int overflow) {
        return overflow > current.size() / 2 
                ? current.size() + overflow * 2
                : current.size();
    }
        
    private final void allocateNewBucket(int size, Instant from) {
        if (!bucketCache.isEmpty() && bucketCache.lastEntry().getValue().isEmpty()) {
            pool.resizeBucket(current, size);
        } else {
            current = pool.getBucket(size, this);
            bucketCache.put(from, current);
        }
    }
    
    protected Message handleOverflow(Message message, int size) throws StreamingException {
        LOG.entry(message);
        allocateNewBucket(calcNewSize(size), message.getTimestamp()); 
        return LOG.exit(current.addMessage(message, this::handleOverflow));
    }
    
    public final Instant now() {
        return clock.instant();
    }
    
    /** Checkpoint guarantees that no buffer entries will be written with a timestamp before the returned value.
     * 
     * Essentially this gets a write lock on the buffer an then gets new timestamp from the buffer clock. This enables
     * us to perform a repeatable read on the buffer by using the value returned by this function as the upper bound.
     * 
     * @return A timestamp 
     */
    public final Instant checkpoint() {
        synchronized(this) {
            return clock.instant();
        }
    }
    
    public final boolean isEmpty() {
        // bucketCache.size() could be an expensive operation so instead we check to see if there
        // is a key higher than the first key. Because keys are never removed from the bucket cache
        // this also be safe from a concurrency perspective
        return current.isEmpty() && bucketCache.higherKey(bucketCache.firstKey()) == null;
    }
    
    public Message addMessage(Message message) {
        LOG.entry(message);
        Message result;
        Instant timestamp;
        try {
            synchronized(this) {
                timestamp = Instant.now(clock);
                Message timestamped = message.setTimestamp(timestamp);
                result = current.addMessage(timestamped, this::handleOverflow);
            }
        } catch (StreamingException e) {
            throw runtime(e);
        } 
        return LOG.exit(result);
    }
    
    public Message[] addMessages(Message... messages) {
        LOG.entry((Object[])messages);
        Message[] result = new Message[messages.length];
        Instant timestamp;
        try {
            synchronized(this) {
                timestamp = Instant.now(clock);
                for (int i = 0; i < messages.length; i++) {
                    Message timestamped = messages[i].setTimestamp(timestamp);
                    result[i] = current.addMessage(timestamped, this::handleOverflow);
                }
            }
        } catch (StreamingException e) {
            throw runtime(e);
        } 
        return LOG.exit(result);
    }
    
    public Optional<Instant> firstTimestamp() {
        return Optional.ofNullable(bucketCache.firstEntry()).flatMap(entry->entry.getValue().firstTimestamp());
    }
    
    /** Get messages after a given timestamp.
     * 
     * Concurrency is per ConcurrentSkipListMap - the Message iterator should contain
     * everything present at the time of calling and may (but is not guaranteed to) contain
     * items added subsequently.
     * 
     * @param timestamp Time from which we are retrieving messages.
     * @return 
     */
    public MessageIterator getMessagesAfter(Instant timestamp, Predicate<Message>... filters) {
        LOG.entry(timestamp);
        Instant searchFrom = bucketCache.floorKey(timestamp);
        Predicate<Message> filter = Stream.of(filters).reduce(message->true,  Predicate::and);
        if (searchFrom == null) searchFrom = timestamp;
        LOG.debug("returning messages from buckets after {}", searchFrom);
        return LOG.exit(
            MessageIterator.of(bucketCache
                .tailMap(searchFrom, true)
                .values()
                .stream()
                .flatMap(bucket->bucket.getMessagesAfter(timestamp))
                .filter(filter)
                .iterator(), ()->{})
        );
    }
    
    /** Get messages between two timestamps.
     * 
     * @param from lower bound 
     * @param fromInclusive lower bound includes given value if true
     * @param to upper bound 
     * @param toInclusive upper bound includes given value if true
     * @param filters predicates used to filter the result 
     * @return 
     */
    public MessageIterator getMessagesBetween(Instant from, boolean fromInclusive, Instant to, boolean toInclusive, Predicate<Message>... filters) {
        LOG.entry(from, fromInclusive, to, toInclusive);
        Instant searchFrom = Optional.ofNullable(bucketCache.floorKey(from)).orElse(from);
        Predicate<Message> filter = Stream.of(filters).reduce(message->true,  Predicate::and);
        LOG.debug("returning messages from buckets between {} and {}", searchFrom, to);
        return LOG.exit(
            MessageIterator.of(bucketCache
                .subMap(searchFrom, true, to, true)
                .values()
                .stream()
                .flatMap(bucket->bucket.getMessagesBetween(from, fromInclusive, to, toInclusive))
                .filter(filter)
                .iterator(), ()->{})
        );
    }
    
    public MessageIterator getMessages(String id, Predicate<Message>... filters) {
        LOG.entry(id);
        Predicate<Message> filter = Stream.of(filters).reduce(message->true,  Predicate::and);
        return LOG.exit(
            MessageIterator.of(
                 bucketCache.values().stream()
                    .flatMap(bucket->bucket.getMessages(id))
                    .filter(filter)
            )
        );
    }
            
    public void dumpState(PrintWriter out) {
        for (Instant bucketStart : bucketCache.keySet()) {
            out.println("Bucket starting: " + bucketStart);
            bucketCache.get(bucketStart).dumpBucket(out);
        }
    }

    void deallocateBucket(Bucket bucket) {
        LOG.entry(bucket);
        // 99.9% of the time we are deallocating old buckets which are nowhere near the insertion point on the buffer.
        // However, this gets very sticky if we have an inactive feed; we could then potentially be trying to deallocate
        // the topmost bucket in the buffer. Which has significant concurrency implications. So what we do is just refuse
        // to deallocate the 'current' bucket if we are asked. We call pool.reallocateBucket instead.
        Map<Instant, Bucket> toRemove;
        synchronized(this) {
            if (bucket == current) {
                pool.reallocateBucket(bucket);
                toRemove = bucketCache.headMap(bucketCache.firstKey(), false);
            } else {
                toRemove = bucketCache.headMap(bucket.firstTimestamp().orElseThrow(()->new RuntimeException("tying to delete bucket with no entries")), true);
            }
        }
        
        pool.releaseBuckets(toRemove.values());
        // toRemove.clear() is not necessarily atomic, and we want to ensure elements are remove
        // from first to last in order to maintain consistency.
        Iterator<Map.Entry<Instant,Bucket>> iterator = toRemove.entrySet().iterator();
        while(iterator.hasNext()) { 
            iterator.next(); 
            iterator.remove(); 
        }
        LOG.exit();
    }    
}
