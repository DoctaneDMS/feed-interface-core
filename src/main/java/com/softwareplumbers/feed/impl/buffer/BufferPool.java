/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import com.softwareplumbers.feed.Message;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/**
 *
 * @author jonathan
 */
public class BufferPool {
    
    private static XLogger LOG = XLoggerFactory.getXLogger(BufferPool.class);
    
    private Object lazy(Supplier<Object> spr) { 
        return new Object() { 
            public String toString() { 
                return spr.get().toString();
            } 
        }; 
    };
    
    private class BucketRegistration {
        public final Bucket bucket;
        public final MessageBuffer buffer;
        public BucketRegistration(Bucket bucket, MessageBuffer buffer) {
            this.bucket = bucket;
            this.buffer = buffer;
        }
    }

    private long maxSize;
    private final AtomicLong currentSize = new AtomicLong(0);
    private final ConcurrentLinkedDeque<BucketRegistration> registry = new ConcurrentLinkedDeque<>();
    
    public BufferPool(long maxSize) {
        this.maxSize = maxSize;
    }
    
    /** Get a bucket from the pool and allocate it to the given buffer.
     * 
     * @param size
     * @param buffer
     * @return 
     */
    Bucket getBucket(int size, MessageBuffer buffer) {
        LOG.entry(size, lazy(()->buffer.now()));
        Bucket bucket = new Bucket(size);
        registry.add(new BucketRegistration(bucket, buffer));
        LOG.debug("pool size: {}", currentSize.addAndGet(size));
        return LOG.exit(bucket);
    }
    
    /** Release buckets from the pool.
     * 
     * @param buckets 
     */
    void releaseBuckets(Collection<Bucket> buckets) {
        LOG.entry(lazy(()->buckets.size()));
        int count = buckets.size();
        Iterator<BucketRegistration> registrations = registry.iterator();
        while (registrations.hasNext() && count > 0) {
            BucketRegistration registration = registrations.next();
            if (buckets.contains(registration.bucket)) {
                count--;
                LOG.debug("pool size: {}", currentSize.addAndGet(-registration.bucket.size()));
                registrations.remove();
            }
        }
        LOG.exit();
    }

    /** Provide a way to continue using an old bucket.
     * 
     * TODO: refactor MessageBuffer so a buffer can exist with no buckets in it. Then this is not needed.
     * 
     * @param bucket Bucket to reallocate.
     */
    void reallocateBucket(Bucket bucket) {
        LOG.entry(bucket);
        boolean more = true;
        Iterator<BucketRegistration> registrations = registry.iterator();
        while (registrations.hasNext() && more) {
            BucketRegistration registration = registrations.next();
            if (bucket == registration.bucket) {
                more = false;
                registrations.remove();
                registry.add(registration);
            }
        }
        LOG.exit();
    }
    
    /** De-allocate buckets.
     * 
     * De-allocate buckets from the pool until the pool size is back under
     * the maxSize threshold.
     * 
     */ 
    public void deallocateBuckets() {
        LOG.entry();
        while (currentSize.get() > maxSize) {
            BucketRegistration registration = registry.getFirst();
            registration.buffer.deallocateBucket(registration.bucket);
        }
        LOG.exit();
    }
    
    void resizeBucket(Bucket bucket, int size) {
        LOG.entry(bucket, size);
        currentSize.addAndGet(size - bucket.size());
        bucket.resize(size);
        LOG.exit();
    }
    
    /** Create a new message buffer in this pool.
     * 
     * Note that if we are to create an aggregate feed across several buffers, it
     * is important that the buffers share the same clock.
     * 
     * @param clock Clock used to create message timestamps
     * @param size Size of buffer to create
     * @return A new message buffer.
     */
    public MessageBuffer createBuffer(MessageClock clock, int size) {
        LOG.entry(size);
        return LOG.exit(new MessageBuffer(this, clock, size));
    }
    
    public long getSize() {
        return currentSize.get();
    }
}
