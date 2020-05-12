/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan
 */
public class BufferPool {
    
    private class BucketRegistration {
        public final Bucket bucket;
        public final MessageBuffer buffer;
        public BucketRegistration(Bucket bucket, MessageBuffer buffer) {
            this.bucket = bucket;
            this.buffer = buffer;
        }
    }

    private long maxSize;
    private AtomicLong currentSize = new AtomicLong(0);
    private ConcurrentLinkedDeque<BucketRegistration> registry = new ConcurrentLinkedDeque<>();
    
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
        Bucket bucket = new Bucket(size);
        registry.add(new BucketRegistration(bucket, buffer));
        currentSize.addAndGet(size);
        return bucket;
    }
    
    /** Release buckets from the pool.
     * 
     * @param buckets 
     */
    void releaseBuckets(Collection<Bucket> buckets) {
        int count = buckets.size();
        Iterator<BucketRegistration> registrations = registry.iterator();
        while (registrations.hasNext() && count > 0) {
            BucketRegistration registration = registrations.next();
            if (buckets.contains(registration.bucket)) {
                count--;
                currentSize.addAndGet(-registration.bucket.size());
                registrations.remove();
            }
        }
    }
    
    /** De-allocate buckets.
     * 
     * De-allocate buckets from the pool until the pool size is back under
     * the maxSize threshold.
     * 
     */ 
    public void deallocateBuckets() {
        while (currentSize.get() > maxSize) {
            BucketRegistration registration = registry.getFirst();
            registration.buffer.deallocateBucket(registration.bucket);
        }
    }
    
    void resizeBucket(Bucket bucket, int size) {
        currentSize.addAndGet(size - bucket.size());
        bucket.resize(size);
    }
    
    public MessageBuffer createBuffer(int size) {
        return new MessageBuffer(this, size);
    }
    
    public long getSize() {
        return currentSize.get();
    }
}
