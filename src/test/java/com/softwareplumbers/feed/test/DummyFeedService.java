/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.test;

import com.softwareplumbers.feed.Cluster;
import com.softwareplumbers.feed.FeedExceptions;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.impl.AbstractFeedService;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

/**
 *
 * @author jonathan
 */
public class DummyFeedService extends AbstractFeedService {
    
    public DummyFeedService(Cluster cluster, long poolSize, int bucketSize) {
        super(cluster, UUID.randomUUID(), Executors.newFixedThreadPool(5), poolSize, bucketSize);
    }

    @Override
    protected String generateMessageId() {
        return UUID.randomUUID().toString();
    }
}
