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
import java.io.Writer;
import java.time.Clock;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
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
    
    public Optional<Instant> firstTimestamp() {
        return bucketCache.isEmpty() ? Optional.empty() : Optional.of(bucketCache.firstEntry().getValue().firstTimestamp());
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
    public MessageIterator getMessagesAfter(Instant timestamp) {
        LOG.entry(timestamp);
        Instant searchFrom = bucketCache.floorKey(timestamp);
        if (searchFrom == null) searchFrom = timestamp;
        LOG.debug("returning messages from buckets after {}", searchFrom);
        return LOG.exit(
            MessageIterator.of(bucketCache
                .tailMap(searchFrom, true)
                .values()
                .stream()
                .flatMap(bucket->bucket.getMessagesAfter(timestamp))
                .iterator(), ()->{})
        );
    }
    
    /** Get messages between two timestamps.
     * 
     * This method actually locks the buffer, preventing writes while the boundaries a calculated.
     * This has the useful side-effect that if 'to' comes from the same clock as this.clock, then
     * the buffer will never contain a message with timestamp before 'to' which was not present in 
     * the returned iterator.
     * 
     * @param from lower bound (gets messages with timestamp greater than this value)
     * @param to upper bound (gets messages with timestamp less than or equal to this value)
     * @return 
     */
    public MessageIterator getMessagesBetween(Instant from, boolean fromInclusive, Instant to, boolean toInclusive) {
        LOG.entry(from, to);
        Instant searchFrom = Optional.ofNullable(bucketCache.floorKey(from)).orElse(from);
        LOG.debug("returning messages from buckets between {} and {}", searchFrom, to);
        return LOG.exit(
            MessageIterator.of(bucketCache
                .subMap(searchFrom, true, to, true)
                .values()
                .stream()
                .flatMap(bucket->bucket.getMessagesBetween(from, fromInclusive, to, toInclusive))
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
        Map<Instant, Bucket> toRemove = bucketCache.headMap(bucket.firstTimestamp(), true);
        pool.releaseBuckets(toRemove.values());
        // toRemove.clear() is not necessarily atomic, and we want to ensure elements are remove
        // from first to last in order to maintain consistency.
        Iterator<Map.Entry<Instant,Bucket>> iterator = toRemove.entrySet().iterator();
        while(iterator.hasNext()) { iterator.next(); iterator.remove(); }
        LOG.exit();
    }    
}
