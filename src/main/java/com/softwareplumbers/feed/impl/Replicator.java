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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
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
    private long timeoutMillis = 10000;
    private ExecutorService callbackExecutor;

    private void monitorCallback(MessageIterator messages, Throwable exception) {
        LOG.entry(messages, exception);
        if (exception != null) {
            LOG.error("Error monitoring {}", from.getServerId(), exception);
            if (messages != null) {
                messages.close();
            }
            lastException = Optional.of(exception);
            errorCount++;
        } else {
            Message message = null;
            try {
                while (messages.hasNext()) {
                    message = messages.next();
                    receivedCount++;
                    to.replicate(message);
                }
                messages.close();
                if (!closed) {
                    from.watch(to.getServerId(), message.getTimestamp(), timeoutMillis).whenCompleteAsync(this::monitorCallback, callbackExecutor);
                }
            } catch (Exception exp) {
                LOG.error("Error {}, monitoring {}", exp, from.getServerId());
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
        LOG.exit();
    }

    public void close() {
        closed = true;
    }

    public void startMonitor() {
        from.watch(to.getServerId(), to.getInitTime(), timeoutMillis).whenComplete(this::monitorCallback);
    }

    public void dumpState(PrintWriter out) {
        out.println(String.format("Replicator: %s -> %s, received: %d, errors: %d, last error: %s", from.getServerId(), to.getServerId(), receivedCount, errorCount, lastException.map(t -> t.getMessage()).orElse("none")));
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
}
