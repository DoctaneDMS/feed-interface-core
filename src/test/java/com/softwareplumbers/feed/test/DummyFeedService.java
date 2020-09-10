/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.test;

import com.softwareplumbers.feed.impl.AbstractFeedService;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 *
 * @author jonathan
 */
public class DummyFeedService extends AbstractFeedService {
    
    public DummyFeedService(long poolSize, int bucketSize) {
        super(UUID.randomUUID(), Executors.newFixedThreadPool(5), poolSize, bucketSize);
    }

    @Override
    protected String generateMessageId() {
        return UUID.randomUUID().toString();
    }
}
