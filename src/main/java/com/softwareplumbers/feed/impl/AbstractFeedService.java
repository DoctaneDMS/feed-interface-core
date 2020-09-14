/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.Cluster;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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
                        replicate(message);
                    }
                    remote.watch(getServerId(), message.getTimestamp()).whenCompleteAsync(this::monitorCallback, callbackExecutor);
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
            LOG.exit();
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
    
    public AbstractFeedService(UUID serverId, ExecutorService callbackExecutor, Instant initTime) {
        this.callbackExecutor = callbackExecutor;
        this.serverId = serverId;
        this.cluster = Cluster.local(this);
        this.initTime = initTime;          
    }
    
    
    @Override
    public void initialize(Cluster cluster) {
        LOG.entry(cluster);
        this.cluster = cluster;
        cluster.getServices(service->!Objects.equals(service.getServerId(), serverId))
            .forEach(this::monitor);
        LOG.exit();
    }
    
    
    @Override
    public void monitor(FeedService remoteService) {
        remotes.computeIfAbsent(remoteService.getServerId(), uuid->new Remote(remoteService)).startMonitor();
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

    public void dumpState(PrintWriter out) throws IOException {
        remotes.values().forEach(remote->remote.dumpState(out));
    }
}
