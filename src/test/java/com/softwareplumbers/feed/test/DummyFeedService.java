/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.test;

import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.impl.AbstractFeedService;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 *
 * @author jonathan
 */
public class DummyFeedService extends AbstractFeedService {

    public DummyFeedService(long poolSize, int bucketSize) {
        super(Executors.newFixedThreadPool(5), poolSize, bucketSize);
    }

    @Override
    protected MessageIterator syncFromBackEnd(FeedPath path, Instant from, Instant to, UUID serverId) {
        return MessageIterator.of(Collections.EMPTY_LIST.iterator(), ()->{});
    }

    @Override
    protected void startBackEndListener(FeedPath path, Instant from) {
    }  
    
    @Override
    protected String getIdFromBackEnd() {
        return UUID.randomUUID().toString();
    }
}
