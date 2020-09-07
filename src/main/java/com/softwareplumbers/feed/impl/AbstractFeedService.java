/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.FeedExceptions.InvalidId;
import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.FeedService;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.impl.buffer.BufferPool;
import com.softwareplumbers.feed.impl.buffer.MessageBuffer;
import com.softwareplumbers.feed.impl.buffer.MessageClock;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/** Implements a basic feed service on top of some other data store.
 * 
 * Uses the MessageBuffer class to implement a circular buffer for messages;
 * abstract methods must be implemented to connect this buffer to a back end
 * message store.
 *
 * @author jonathan
 */
public abstract class AbstractFeedService implements FeedService {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(AbstractFeedService.class);
    
    private final BufferPool bufferPool;
    private final ExecutorService callbackExecutor;
    private final AbstractFeed rootFeed;
    private final int bucketSize;
    private final UUID serverId = UUID.randomUUID();
    private final MessageClock clock = new MessageClock();
    private final long ackTimeout = 600; // 10 minutes

   
    /** Create a new Abstract Feed Service.
     * 
     * @param callbackExecutor Executor service for running callbacks
     * @param poolSize Amount of memory in bytes to dedicate to all message buffers
     * @param bucketSize Size of a bucket - should be larger than the expected maximum message size
     */
    public AbstractFeedService(ExecutorService callbackExecutor, long poolSize, int bucketSize) {
        this.bufferPool = new BufferPool((int)poolSize);
        this.bucketSize = bucketSize; 
        this.callbackExecutor = callbackExecutor;
        this.rootFeed = new AbstractFeed(this);
    }
    
    void callback(Runnable callback) {
        callbackExecutor.submit(callback);
    }
    
    MessageBuffer createBuffer() {
        return bufferPool.createBuffer(clock, bucketSize);
    }
    
    MessageClock getClock() {
        return clock;
    }
    
    long getAckTimeout() {
        return ackTimeout;
    }

    @Override
    public CompletableFuture<MessageIterator> listen(FeedPath path, Instant from) throws InvalidPath {
        LOG.entry(path, from);
        return LOG.exit(rootFeed.getFeed(path).listen(this, from));
    }
    
    @Override
    public MessageIterator sync(FeedPath path, Instant from, UUID serverId) throws InvalidPath {
        LOG.entry(path, from, serverId);
        return LOG.exit(rootFeed.getFeed(path).sync(from, false, clock.instant(), true, serverId));

    }
    
    @Override
    public Message post(FeedPath path, Message message) throws InvalidPath {
        LOG.entry(path, message);
        return LOG.exit(rootFeed.getFeed(path).post(message));
    }
    
    @Override
    public UUID getServerId() {
        return serverId;
    }
        
    @Override
    public MessageIterator getMessages(FeedPath path) throws InvalidPath, InvalidId {
        if (path.isEmpty()) throw new InvalidPath(path);
        String id = path.part.getId().orElseThrow(()->new InvalidId(path.parent, path.part.toString()));
        return rootFeed.getFeed(path.parent).getMessages(id);
    }
    
    public void dumpState(FeedPath path, PrintWriter out) throws IOException {
        LOG.entry(path, out);
        Iterator<AbstractFeed> feeds = rootFeed.getLiveFeeds().iterator();
        while (feeds.hasNext()) feeds.next().dumpState(out);
    }
    
    protected abstract String generateMessageId();
    protected abstract MessageIterator syncFromBackEnd(FeedPath path, Instant from, boolean fromInclusive, Instant to, boolean toInclusive, UUID serverId) throws InvalidPath;   
    protected abstract void startBackEndListener(FeedPath path, Instant from) throws InvalidPath;
}
