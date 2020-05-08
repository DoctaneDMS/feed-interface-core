/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.Message;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
import javax.json.JsonObject;

/**
 *
 * @author jonathan
 */
public class MessageBuffer {
        
    private final TreeMap<Instant, Bucket> bucketCache = new TreeMap<>();
    private final HashMap<String, Message> idIndex = new HashMap<>();
    private Bucket current;
    private BucketPool pool;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    
    MessageBuffer(BucketPool pool, int initSize) {
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
    
    protected Message addMessage(FeedPath path, Instant time, InputStream overflow, InputStream data) throws IOException {
        try {
            return current.addMessage(path, time, overflow, data);
        } catch (MessageOverflow movr) {
            allocateNewBucket(calcNewSize(movr.getSize()), time); 
            return addMessage(path, time, movr.getOverflow().get(), data);
        }
    }

    
    public  Message addMessage(FeedPath path, Instant time, JsonObject allHeaders, InputStream data) throws IOException {
        try {
            lock.writeLock().lock();
            Message message = current.addMessage(path, time, allHeaders, data);
            idIndex.put(path.part.getId().orElseThrow(() -> new RuntimeException("Invalid Id on path")), message);
            return message;
        } catch (HeaderOverflow hovr) {
            allocateNewBucket(calcNewSize(hovr.getSize()), time);
            return addMessage(path, time, allHeaders, data);
        } catch (MessageOverflow movr) {
            allocateNewBucket(calcNewSize(movr.getSize()), time); 
            return addMessage(path, time, movr.getOverflow().get(), data);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public Message addMessage(Message message) {
        try {
            return addMessage(message.getName(), message.getTimestamp(), message.header(), message.getData());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    
    public Stream<Message> getMessagesAfter(Instant timestamp) {
        try {
            lock.readLock().lock();
            // search starts from the first bucket less than timestamp. This is
            // because it is theoretically possible for a bucket to end with
            // a message with the same timestamp as the first message in a new
            // bucket.
            Instant searchFrom = bucketCache.lowerKey(timestamp);
            if (searchFrom == null) searchFrom = timestamp;
            return bucketCache
                .tailMap(searchFrom, true)
                .values()
                .stream()
                .flatMap(bucket->bucket.getMessagesAfter(timestamp))
                .onClose(()->lock.readLock().unlock());
        } catch(RuntimeException e) {
            lock.readLock().unlock();
            throw e;
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

            toRemove.values().stream().sequential()
                .flatMap(bkt->bkt.getMessages())
                .forEach(msg->idIndex.remove(msg.getId()));

            pool.releaseBuckets(toRemove.values());
            toRemove.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }    
}
