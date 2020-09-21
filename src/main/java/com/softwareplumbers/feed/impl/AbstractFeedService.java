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
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
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

/**
 *
 * @author jonat
 */
public abstract class AbstractFeedService implements FeedService {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(AbstractFeedService.class);
    
    private class Remote {
        
        private final FeedService remote;
        private Optional<Throwable> lastException = Optional.empty();
        private long receivedCount = 0;
        private long errorCount = 0;
        private boolean closed;
        
        private void monitorCallback(MessageIterator messages, Throwable exception) {
            LOG.entry(messages, exception);
            if (exception != null) {
                LOG.error("Error monitoring {}", remote.getServerId(), exception);
                if (messages != null) messages.close();
                lastException = Optional.of(exception);
                errorCount++;
            } else {
                Message message = null;
                try {
                    while (messages.hasNext()) {
                        message = messages.next();
                        receivedCount++;
                        replicate(message);
                    }
                    messages.close();
                    if (!closed) remote.watch(getServerId(), message.getTimestamp()).whenCompleteAsync(this::monitorCallback, callbackExecutor);
                } catch (Exception exp) {
                    LOG.error("Error monitoring {}", remote.getServerId());
                    lastException = Optional.of(exp);
                    errorCount++;
                }
            }
            LOG.exit();
        }

        public Remote(FeedService remote) {
            LOG.entry(remote);
            this.remote = remote;
            this.closed = false;
            LOG.exit();
        }    
        
        public void close() {
            closed = true;
        }
        
        public void startMonitor() {
            remote.watch(getServerId(), initTime).whenComplete(this::monitorCallback);            
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

    private final ExecutorService callbackExecutor;
    protected final UUID serverId;
    private final Map<UUID, Remote> remotes = new ConcurrentHashMap<>();
    protected Cluster cluster;
    protected final Instant initTime;
    private final AbstractFeed rootFeed;
    private final long ackTimeout = 600; // 10 minutes

    public AbstractFeedService(UUID serverId, ExecutorService callbackExecutor, Instant initTime, AbstractFeed rootFeed) {
        this.callbackExecutor = callbackExecutor;
        this.serverId = serverId;
        this.cluster = Cluster.local(this);
        this.initTime = initTime;          
        this.rootFeed = rootFeed;

    }
    
    public long getAckTimeout() {
        return ackTimeout;
    }
    
    @Override
    public void initialize(Cluster cluster) {
        LOG.entry(cluster);
        LOG.debug("Initializing feed service {}", this);
        this.cluster = cluster;
        cluster.getServices(Cluster.Filters.idIsNot(serverId))
            .forEach(this::monitor);
        LOG.exit();
    }
    
    @Override
    public void close() {
        LOG.entry();
        remotes.values().forEach(Remote::close);
        remotes.clear();
        cluster.deregister(this);
        LOG.exit();
    }
    
    @Override
    public void monitor(FeedService remoteService) {
        LOG.entry(remoteService);
        LOG.debug("Service {} will monitor {}", this, remoteService);
        remotes.computeIfAbsent(remoteService.getServerId(), uuid->new Remote(remoteService)).startMonitor();
        LOG.exit();
    }
    
    void callback(Runnable callback) {
        LOG.entry(callback);
        callbackExecutor.submit(callback);
        LOG.exit();
    }    
    
    @Override
    public Cluster getCluster() {
        return cluster;
    }
    
    @Override
    public Instant getInitTime() {
        return initTime;
    }
    
    @Override
    public Stream<Feed> getFeeds() {
        return rootFeed.getDescendents().map(Feed.class::cast);
    }

    public void dumpState(PrintWriter out) throws IOException {
        remotes.values().forEach(remote->remote.dumpState(out));
        getFeeds().forEachOrdered(feed->((AbstractFeed)feed).dumpState(out));
    }
    
    @Override
    public Feed getFeed(FeedPath path) throws FeedExceptions.InvalidPath {
        return rootFeed.getFeed(this, path);
    }
    
    public abstract AbstractFeed createFeed(AbstractFeed parent, String name);
    
    @Override
    public CompletableFuture<MessageIterator> watch(UUID serverId, Instant from) {
        LOG.entry(serverId);
        LOG.trace("{} is being watched by {}", this.serverId, serverId);
        return LOG.exit(rootFeed.watch(this, from));
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
    public Message post(FeedPath path, Message message) throws FeedExceptions.InvalidPath {
        LOG.entry(path, message);
        return LOG.exit(getFeed(path).post(this, message));
    }
    
    @Override
    public Message replicate(Message message) {
        LOG.entry(message);
        try {
            return LOG.exit(getFeed(message.getFeedName()).replicate(this, message));
        } catch (FeedExceptions.InvalidPath e) {
            throw FeedExceptions.runtime(e);
        }
    }

    @Override
    public CompletableFuture<MessageIterator> listen(FeedPath path, Instant from, UUID serverId, Predicate<Message>... filters) throws FeedExceptions.InvalidPath {
        LOG.entry(path, from);
        return LOG.exit(getFeed(path).listen(this, from, serverId, filters));
    }
    
    @Override
    public MessageIterator search(FeedPath path, UUID serverId, Instant from, boolean fromInclusive, Optional<Instant> to, Optional<Boolean> toInclusive, Optional<Boolean> relay, Predicate<Message>... filters) throws FeedExceptions.InvalidPath {
        LOG.entry(path, from, fromInclusive, to, toInclusive, serverId);
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
    public String toString() {
        return "FeedService[" + getServerId() + "]";
    }
}
