/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.Cluster;
import com.softwareplumbers.feed.Feed;
import com.softwareplumbers.feed.FeedExceptions;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.FeedService;
import com.softwareplumbers.feed.FeedServiceManager;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/**
 *
 * @author jonat
 */
public abstract class AbstractFeedService implements FeedService {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(AbstractFeedService.class);
    
    private final ScheduledExecutorService callbackExecutor;
    protected final UUID serverId;
    protected Optional<FeedServiceManager> manager;
    protected final Instant initTime;
    private final AbstractFeed rootFeed;
    private final long ackTimeout = 600; // 10 minutes
    private volatile boolean closing = false;
    
    public AbstractFeedService(UUID serverId, ScheduledExecutorService callbackExecutor, Instant initTime, AbstractFeed rootFeed) {
        this.callbackExecutor = callbackExecutor;
        this.serverId = serverId;
        this.manager = Optional.empty();
        this.initTime = initTime;          
        this.rootFeed = rootFeed;
    }
    
    public long getAckTimeout() {
        return ackTimeout;
    }
     
    @Override
    public void setManager(FeedServiceManager manager) {
        LOG.entry(manager);
        LOG.debug("Initializing feed service {}", this);
        this.manager = Optional.of(manager);
        LOG.exit();
    }
    
    @Override
    public void close() throws Exception {
        LOG.entry();
        closing = true;
        manager.ifPresent(c->c.deregister(this));
        callbackExecutor.shutdown();
        LOG.exit();
    }
        
    void callback(Runnable callback) {
        LOG.entry(callback);
        callbackExecutor.submit(callback);
        LOG.exit();
    }  
    
    void schedule(Runnable callback, long delayMillis) {
        LOG.entry(callback, delayMillis);
        callbackExecutor.schedule(callback, delayMillis, TimeUnit.MILLISECONDS);
        LOG.exit();        
    }
    
    @Override
    public Optional<Cluster> getCluster() {
        return manager.map(FeedServiceManager::getCluster);
    }
    
    @Override
    public Instant getInitTime() {
        return initTime;
    }
    
    @Override
    public Stream<Feed> getFeeds() {
        return rootFeed.getDescendents().map(Feed.class::cast);
    }
    
    @Override
    public Stream<Feed> getChildren(FeedPath path) throws FeedExceptions.InvalidPath {
        return getFeed(path).getChildren(this);
    }
    
    @Override
    public Feed getFeed(FeedPath path) throws FeedExceptions.InvalidPath {
        return rootFeed.getFeed(this, path);
    }
    
    public abstract AbstractFeed createFeed(AbstractFeed parent, String name);
    
    @Override
    public CompletableFuture<MessageIterator> watch(UUID serverId, Instant from, long timeoutMillis) {
        LOG.entry(serverId);
        LOG.trace("{} is being watched by {}", this.serverId, serverId);
        return LOG.exit(rootFeed.watch(this, from, timeoutMillis));
    }    
    
        
    @Override
    public UUID getServerId() {
        return serverId;
    }
    
    protected abstract String generateMessageId();
    
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
    public Message post(FeedPath path, Message message) throws FeedExceptions.InvalidPath, FeedExceptions.InvalidState {
        LOG.entry(path, message);
        if (closing) throw new FeedExceptions.InvalidState("Feed is closing");
        return LOG.exit(getFeed(path).post(this, message));
    }
    
    @Override
    public Message replicate(Message message) throws FeedExceptions.InvalidState {
        LOG.entry(message);
        if (closing) throw new FeedExceptions.InvalidState("Feed is closing");
        try {
            return LOG.exit(getFeed(message.getFeedName()).replicate(this, message));
        } catch (FeedExceptions.InvalidPath e) {
            throw FeedExceptions.runtime(e);
        }
    }

    @Override
    public CompletableFuture<MessageIterator> listen(FeedPath path, Instant from, UUID serverId, long timeoutMillis, Predicate<Message>... filters) throws FeedExceptions.InvalidPath {
        LOG.entry(path, from);
        return LOG.exit(getFeed(path).listen(this, from, serverId, timeoutMillis, filters));
    }
    
    @Override
    public MessageIterator search(FeedPath path, UUID serverId, Instant from, boolean fromInclusive, Optional<Instant> to, Optional<Boolean> toInclusive, Optional<Boolean> relay, Predicate<Message>... filters) throws FeedExceptions.InvalidPath {
        LOG.entry(path, serverId, from, fromInclusive, to, toInclusive, relay);
        return LOG.exit(getFeed(path).search(this, serverId, from, fromInclusive, to, toInclusive, relay, filters));
    }
            
    @Override
    public MessageIterator search(FeedPath path, Predicate<Message>... filters) throws FeedExceptions.InvalidPath, FeedExceptions.InvalidId {
        LOG.entry(path, filters);
        if (path.isEmpty()) throw new FeedExceptions.InvalidPath(path);
        String id = path.part.getId().orElseThrow(()->new FeedExceptions.InvalidId(path.parent, path.part.toString()));
        return LOG.exit(getFeed(path.parent).search(this, id, filters));
    }  
    
    @Override
    public Optional<Instant> getLastTimestamp(FeedPath path) throws FeedExceptions.InvalidPath {
        return getFeed(path).getLastTimestamp(this);
    }
    
    @Override
    public String toString() {
        return "FeedService[" + getServerId() + "]";
    }
    
    public void dumpState(PrintWriter out) {
        getFeeds().forEachOrdered(feed->((AbstractFeed)feed).dumpState(out));        
    }
}
