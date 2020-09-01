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
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
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
    private final Clock clock = new MessageClock();
    private final CallbackRegistry callbacks;
    
    MessageBuffer(ExecutorService callbackExecutor, BufferPool pool, int initSize) {
        this.pool = pool;
        this.callbacks = new CallbackRegistry(callbackExecutor);
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
        callbacks.notify(timestamp, this::getMessagesAfter);
        return LOG.exit(result);
    }
    
    public Optional<Instant> firstTimestamp() {
        return bucketCache.isEmpty() ? Optional.empty() : Optional.of(bucketCache.firstEntry().getValue().firstTimestamp());
    }
    
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
    
    public CompletableFuture<MessageIterator> getFutureMessagesAfter(Instant timestamp) {
        LOG.entry(timestamp);
        try (MessageIterator found = getMessagesAfter(timestamp)) {
            if (found.hasNext()) {
                LOG.debug("immediately returning messages");
                return LOG.exit(CompletableFuture.completedFuture(found));
            } else {
                return LOG.exit(callbacks.submitSearch(timestamp));
            }
        }
    }
            
    public void dumpBuffer() {
        for (Instant bucketStart : bucketCache.keySet()) {
            System.out.println("Bucket starting: " + bucketStart);
            bucketCache.get(bucketStart).dumpBucket();
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
