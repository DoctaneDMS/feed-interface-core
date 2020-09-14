/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.Feed;
import com.softwareplumbers.feed.FeedExceptions;
import com.softwareplumbers.feed.FeedExceptions.InvalidId;
import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.impl.buffer.BufferPool;
import com.softwareplumbers.feed.impl.buffer.MessageBuffer;
import com.softwareplumbers.feed.impl.buffer.MessageClock;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
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
public abstract class BufferingFeedService extends AbstractFeedService {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(BufferingFeedService.class);
    
    private final BufferPool bufferPool;
    private final BufferingFeed rootFeed;
    private final int bucketSize;
    private final long ackTimeout = 600; // 10 minutes
    private final MessageClock clock;

    /** Create a new Abstract Feed Service.
     * 
     * @param serverId An identifier for this service
     * @param callbackExecutor Executor service for running callbacks
     * @param poolSize Amount of memory in bytes to dedicate to all message buffers
     * @param bucketSize Size of a bucket - should be larger than the expected maximum message size
     */
    public BufferingFeedService(UUID serverId, ExecutorService callbackExecutor, MessageClock clock, long poolSize, int bucketSize) {
        super(serverId, callbackExecutor, clock.instant());
        this.bufferPool = new BufferPool((int)poolSize);
        this.bucketSize = bucketSize; 
        this.rootFeed = new BufferingFeed(bufferPool.createBuffer(clock, bucketSize));
        this.clock = clock;            
    }
    
    final MessageBuffer createBuffer() {
        return bufferPool.createBuffer(clock, bucketSize);
    }
    
    MessageClock getClock() {
        return clock;
    }
    
    long getAckTimeout() {
        return ackTimeout;
    }
    

    @Override
    public CompletableFuture<MessageIterator> listen(FeedPath path, Instant from, UUID serverId, Predicate<Message>... filters) throws InvalidPath {
        LOG.entry(path, from);
        return LOG.exit(rootFeed.getFeed(this, path).listen(this, from, serverId, filters));
    }
    
    @Override
    public CompletableFuture<MessageIterator> watch(UUID serverId, Instant from) {
        LOG.entry(serverId);
        return LOG.exit(rootFeed.watch(this, from));
    }

    @Override
    public MessageIterator search(FeedPath path, Instant from, UUID serverId, boolean relay, Predicate<Message>... filters) throws InvalidPath {
        LOG.entry(path, from, serverId);
        return LOG.exit(rootFeed.getFeed(this, path).search(this, from, serverId, relay, filters));

    }
    
    @Override
    public MessageIterator search(FeedPath path, Instant from, boolean fromInclusive, Instant to, boolean toInclusive, UUID serverId, boolean relay, Predicate<Message>... filters) throws InvalidPath {
        LOG.entry(path, from, fromInclusive, to, toInclusive, serverId);
        return LOG.exit(rootFeed.getFeed(this, path).search(this, from, fromInclusive, to, toInclusive, serverId, relay, filters));
    }
    
    /** Post a message.
     * 
     * The given message is submitted to the message buffer.
     * 
     * @param path
     * @param message
     * @return
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    @Override
    public Message post(FeedPath path, Message message) throws InvalidPath {
        LOG.entry(path, message);
        return LOG.exit(rootFeed.getFeed(this, path).post(this, message));
    }
    
    @Override
    public Message replicate(Message message) {
        LOG.entry(message);
        try {
            return LOG.exit(rootFeed.getFeed(this, message.getFeedName()).replicate(this, message));
        } catch (InvalidPath e) {
            throw FeedExceptions.runtime(e);
        }
    }
    
    @Override
    public UUID getServerId() {
        return serverId;
    }
        
    @Override
    public MessageIterator search(FeedPath path, Predicate<Message>... filters) throws InvalidPath, InvalidId {
        LOG.entry(path, filters);
        if (path.isEmpty()) throw new InvalidPath(path);
        String id = path.part.getId().orElseThrow(()->new InvalidId(path.parent, path.part.toString()));
        return LOG.exit(rootFeed.getFeed(this, path.parent).search(this, id, filters));
    }
    
    @Override
    public Feed getFeed(FeedPath path) throws InvalidPath {
        return rootFeed.getFeed(this, path);
    }
    
    public void dumpState(PrintWriter out) throws IOException {
        LOG.entry(out);
        rootFeed.getLiveFeeds().forEachOrdered(feed->feed.dumpState(out));
        super.dumpState(out);
        LOG.exit();
    }
    
    protected abstract String generateMessageId();
}
