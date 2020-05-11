/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import com.softwareplumbers.feed.Message;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import javax.json.JsonObject;

/**
 *
 * @author jonathan
 */
public class MessageBuffer {
        
    private final TreeMap<Instant, Bucket> bucketCache = new TreeMap<>();
    private Bucket current;
    private BufferPool pool;
    private Clock clock = new MessageClock();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private CallbackRegistry callbacks = new CallbackRegistry();
    
    MessageBuffer(BufferPool pool, int initSize) {
        this.pool = pool;
        allocateNewBucket(initSize, Instant.now());
    }
    
    protected int calcNewSize(int overflow) {
        return overflow > current.size() / 2 
                ? current.size() + overflow * 2
                : current.size();
    }
        
    protected void allocateNewBucket(int size, Instant from) {
        if (!bucketCache.isEmpty() && bucketCache.lastEntry().getValue().isEmpty()) {
            pool.resizeBucket(current, size);
        } else {
            current = pool.getBucket(size, this);
            bucketCache.put(from, current);
        }
    }
    
    protected Message handleOverflow(Message message, int size, InputStream recovered) throws IOException {
        allocateNewBucket(calcNewSize(size), message.getTimestamp()); 
        if (recovered == null)
           return current.addMessage(message, (is, sz)->handleOverflow(message, sz, is));
        else
           return current.addMessage(message.getTimestamp(), recovered, (is, sz)->handleOverflow(message, sz, is));        
    }
    
    public Message addMessage(Message message) {
        Message timestamped = message.setTimestamp(Instant.now(clock));
        Message result;
        try {
            lock.writeLock().lock();
            result = current.addMessage(timestamped, (is, sz)->handleOverflow(timestamped, sz, is));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
        callbacks.callback(this::getMessagesAfter);
        return result;
    }
    
    public MessageIterator getMessagesAfter(Instant timestamp) {
        try {
            lock.readLock().lock();
            Instant searchFrom = bucketCache.floorKey(timestamp);
            if (searchFrom == null) searchFrom = timestamp;
            return new MessageIterator(bucketCache
                .tailMap(searchFrom, true)
                .values()
                .stream()
                .flatMap(bucket->bucket.getMessagesAfter(timestamp))
                .iterator(), ()->lock.readLock().unlock());
        } catch(RuntimeException e) {
            throw e;
        } 
    }
    
    public void getMessagesAfter(Instant timestamp, Consumer<MessageIterator> callback) {
        try (MessageIterator found = getMessagesAfter(timestamp)) {
            if (found.hasNext()) {
                callback.accept(found);
            } else {
                callbacks.addCallback(timestamp, callback);
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
        try {
            lock.writeLock().lock();
            Map<Instant, Bucket> toRemove = bucketCache.headMap(bucket.firstTimestamp(), true);
            pool.releaseBuckets(toRemove.values());
            toRemove.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }    
}
