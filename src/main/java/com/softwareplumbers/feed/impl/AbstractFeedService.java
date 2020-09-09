/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.Cluster;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.stream.Stream;
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
    private final UUID serverId;
    private final MessageClock clock = new MessageClock();
    private final long ackTimeout = 600; // 10 minutes
    private final Map<UUID, Remote> remotes = new ConcurrentHashMap<>();
    private final Cluster cluster;

    private final Predicate<Message> THIS_SERVER = message->Objects.equals(message.getServerId(),getServerId());

    /** Create a new Abstract Feed Service.
     * 
     * @param cluster Cluster to which this service belongs
     * @param callbackExecutor Executor service for running callbacks
     * @param poolSize Amount of memory in bytes to dedicate to all message buffers
     * @param bucketSize Size of a bucket - should be larger than the expected maximum message size
     */
    public AbstractFeedService(Cluster cluster, UUID serverId, ExecutorService callbackExecutor, long poolSize, int bucketSize) {
        this.bufferPool = new BufferPool((int)poolSize);
        this.bucketSize = bucketSize; 
        this.callbackExecutor = callbackExecutor;
        this.rootFeed = new AbstractFeed(createBuffer());
        this.cluster = cluster;
        this.serverId = serverId;
        cluster.init(this);
    }
    
    public void registerRemote(FeedService remoteService) {
        LOG.entry(remoteService);
        remotes.computeIfAbsent(remoteService.getServerId(), uuid->new Remote(remoteService));
        LOG.exit();
    }
    
    void callback(Runnable callback) {
        LOG.entry(callback);
        callbackExecutor.submit(callback);
        LOG.exit();
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
    
    public Cluster getCluster() {
        return cluster;
    }

    @Override
    public CompletableFuture<MessageIterator> listen(FeedPath path, Instant from, UUID serverId, Predicate<Message>... filters) throws InvalidPath {
        LOG.entry(path, from);
        return LOG.exit(rootFeed.getFeed(this, path).listen(this, from, serverId, filters));
    }
    
    @Override
    public CompletableFuture<MessageIterator> watch(UUID serverId, Instant from) {
        LOG.entry(serverId);
        registerRemote(cluster.getService(serverId).orElseThrow(()->new RuntimeException("Invalid server Id" + serverId)));
        return LOG.exit(rootFeed.listen(this, from, getServerId(), THIS_SERVER));
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
    
    @Override
    public Message post(FeedPath path, Message message) throws InvalidPath {
        LOG.entry(path, message);
        return LOG.exit(rootFeed.getFeed(this, path).post(this, message));
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
    
    public void dumpState(FeedPath path, PrintWriter out) throws IOException {
        LOG.entry(path, out);
        rootFeed.getLiveFeeds().forEachOrdered(feed->feed.dumpState(out));
        remotes.values().forEach(remote->remote.dumpState(out));
        LOG.exit();
    }
    
    protected abstract String generateMessageId();

    private class Remote {
        
        private final FeedService remote;
        private Optional<Throwable> lastException = Optional.empty();
        private long receivedCount = 0;
        private long errorCount = 0;
        
        private void monitorCallback(MessageIterator messages, Throwable exception) {
            LOG.entry(messages, exception);
            if (exception != null) {
                LOG.error("Error monitoring {}", remote.getServerId(), exception);
                lastException = Optional.of(exception);
                errorCount++;
            } else {
                Message message = null;
                try {
                    while (messages.hasNext()) {
                        message = messages.next();
                        receivedCount++;
                        post(message.getFeedName(), message);
                    }
                    remote.watch(getServerId(), message.getTimestamp()).whenCompleteAsync(this::monitorCallback);
                } catch (Exception exp) {
                    LOG.error("Error monitoring {}", remote.getServerId());
                    lastException = Optional.of(exp);
                    errorCount++;
                }
            }
            LOG.exit();
        }

        public void monitor(Instant from) {
            LOG.entry();
            remote.watch(getServerId(), from).whenComplete(this::monitorCallback);
            LOG.exit();
        }
        
        public Remote(FeedService remote) {
            LOG.entry(remote);
            this.remote = remote;
            LOG.exit();
        }    
        
        public void dumpState(PrintWriter out) {
            out.println(String.format(
                "Remote: %s, received: %d, errors: %d, last error: %s", 
                remote.getServerId(), 
                receivedCount, 
                errorCount, 
                lastException.map(t->t.getMessage()).orElse("none")
            ));
        }
    }
}
