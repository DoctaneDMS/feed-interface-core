/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.test;

import com.softwareplumbers.feed.impl.BufferingFeedService;
import com.softwareplumbers.feed.impl.buffer.BufferPool;
import com.softwareplumbers.feed.impl.buffer.MessageClock;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 *
 * @author jonathan
 */
public class DummyFeedService extends BufferingFeedService {
    
    public DummyFeedService(long poolSize, int bucketSize) {
        super(UUID.randomUUID(), Executors.newFixedThreadPool(5), new MessageClock(), new BufferPool(poolSize), bucketSize);
    }

    @Override
    protected String generateMessageId() {
        return UUID.randomUUID().toString();
    }
}
