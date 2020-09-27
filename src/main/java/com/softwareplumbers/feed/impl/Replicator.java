/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.FeedService;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/**
 *
 * @author jonat
 */
class Replicator {

    private static final XLogger LOG = XLoggerFactory.getXLogger(Replicator.class);
   
    private final FeedService from;
    private final FeedService to;
    private Optional<Throwable> lastException = Optional.empty();
    private long receivedCount = 0;
    private long errorCount = 0;
    private boolean closed;
    private final long timeoutMillis = 5000;
    private Instant pollingFrom;
    private final ExecutorService callbackExecutor;
    private CompletableFuture<?> watcher;

    private void handleMessages(MessageIterator messages, Throwable exception) {
        LOG.entry(messages, exception);
        if (exception != null) {
            LOG.error("Error monitoring {}", from.getServerId(), exception);
            if (messages != null) {
                messages.close();
            }
            lastException = Optional.of(exception);
            errorCount++;
        } else {
            try {
                while (messages.hasNext()) {
                    Message message = messages.next();
                    receivedCount++;
                    pollingFrom = message.getTimestamp();
                    to.replicate(message);
                }
                messages.close();
                synchronized(this) {
                    if (!closed) {
                        watcher = from.watch(to.getServerId(), pollingFrom, timeoutMillis).whenCompleteAsync(this::handleMessages, callbackExecutor);
                    }
                }
            } catch (Exception exp) {
                LOG.error("Error monitoring {}", from.getServerId(), exp);
                lastException = Optional.of(exp);
                errorCount++;
            }
        }
        LOG.exit();
    }

    public Replicator(ExecutorService callbackExecutor, FeedService to, FeedService from) {
        LOG.entry(callbackExecutor, to, from);
        this.from = from;
        this.to = to;
        this.callbackExecutor = callbackExecutor;
        this.closed = false;
        this.pollingFrom = to.getInitTime();
        LOG.exit();
    }

    public void close() {
        LOG.entry();
        CompletableFuture<?> waitOn;
        
        synchronized(this) {
            closed = true;
            waitOn = watcher;
        }
        
        try {
            waitOn.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            throw new RuntimeException(ex);
        }
        LOG.exit();
    }
    


    public void startMonitor() {
        watcher = from.watch(to.getServerId(), to.getInitTime(), timeoutMillis).whenComplete(this::handleMessages);
    }

    public void dumpState(PrintWriter out) {
        out.println(String.format("Replicator: %s -> %s, received: %d, errors: %d, last error: %s", from.getServerId(), to.getServerId(), receivedCount, errorCount, lastException.map(t -> t.getMessage()).orElse("none")));
        lastException.ifPresent(e->e.printStackTrace(out));
    }
    
    @Override
    public int hashCode() {
        return to.getServerId().hashCode() ^ from.getServerId().hashCode();
    }
    
    public FeedService getFrom() { 
        return from;
    }
    
    public FeedService getTo() {
        return to;
    }
    
    public boolean equals(Replicator other) {
        return to.getServerId().equals(other.to.getServerId()) && from.getServerId().equals(other.from.getServerId());
    } 
    
    public boolean equals(Object other) {
        return other instanceof Replicator && equals((Replicator)other);
    }
    
    public boolean touches(UUID serviceId) {
        return from.getServerId().equals(serviceId) || to.getServerId().equals(serviceId);
    }
}
