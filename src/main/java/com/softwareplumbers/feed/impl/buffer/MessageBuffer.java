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
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
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
    
    private final TreeMap<Instant, Bucket> bucketCache = new TreeMap<>();
    private Bucket current;
    private final BufferPool pool;
    private final Clock clock = new MessageClock();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final CallbackRegistry callbacks = new CallbackRegistry();
    
    MessageBuffer(BufferPool pool, int initSize) {
        this.pool = pool;
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
        try {
            lock.readLock().lock();
            return current.isEmpty() && bucketCache.size() == 1;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public Message addMessage(Message message) {
        LOG.entry(message);
        Message result;
        try {
            lock.writeLock().lock();
            Message timestamped = message.setTimestamp(Instant.now(clock));
            result = current.addMessage(timestamped, this::handleOverflow);
        } catch (StreamingException e) {
            throw runtime(e);
        } finally {
            lock.writeLock().unlock();
        }
        callbacks.callback(this::getMessagesAfter);
        return LOG.exit(result);
    }
    
    public Optional<Instant> firstTimestamp() {
        lock.readLock().lock();
        try {
            return bucketCache.isEmpty() ? Optional.empty() : Optional.of(bucketCache.firstEntry().getValue().firstTimestamp());
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public MessageIterator getMessagesAfter(Instant timestamp) {
        LOG.entry(timestamp);
        lock.readLock().lock();
        Instant searchFrom = bucketCache.floorKey(timestamp);
        if (searchFrom == null) searchFrom = timestamp;
        // NOTE read lock held until iterator is closed
        LOG.debug("returning messages from buckets after {}", searchFrom);
        return LOG.exit(
            MessageIterator.of(bucketCache
                .tailMap(searchFrom, true)
                .values()
                .stream()
                .flatMap(bucket->bucket.getMessagesAfter(timestamp))
                .iterator(), ()->lock.readLock().unlock())
        );
    }
    
    public void getMessagesAfter(Instant timestamp, Consumer<MessageIterator> callback) {
        LOG.entry(timestamp, callback);
        try (MessageIterator found = getMessagesAfter(timestamp)) {
            if (found.hasNext()) {
                LOG.debug("immediately returning messages");
                callback.accept(found);
            } else {
                callbacks.addCallback(timestamp, callback);
            }
        }
        LOG.exit();
    }

    public void cancelCallback(Consumer<MessageIterator> callback) {
        callbacks.cancel(callback);
    }
    
    public void dumpBuffer() {
        for (Instant bucketStart : bucketCache.keySet()) {
            System.out.println("Bucket starting: " + bucketStart);
            bucketCache.get(bucketStart).dumpBucket();
        }
    }

    void deallocateBucket(Bucket bucket) {
        LOG.entry(bucket);
        try {
            lock.writeLock().lock();
            Map<Instant, Bucket> toRemove = bucketCache.headMap(bucket.firstTimestamp(), true);
            pool.releaseBuckets(toRemove.values());
            toRemove.clear();
        } finally {
            lock.writeLock().unlock();
        }
        LOG.exit();
    }    
}
