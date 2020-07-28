/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import com.softwareplumbers.feed.MessageIterator;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/**
 *
 * @author jonathan
 */
public class CallbackRegistry {
    
    private static XLogger LOG = XLoggerFactory.getXLogger(BufferPool.class);
    
    private static class Entry {
        public final Instant from;
        public final Consumer<MessageIterator> callback;
        public Entry(Instant from, Consumer<MessageIterator> callback) {
            this.from = from;
            this.callback = callback;
        }
    }
    
    HashMap<Consumer<MessageIterator>, Instant> callbacks = new HashMap<>();
    
    public void addCallback(Instant from, Consumer<MessageIterator> callback) {
        LOG.entry(from, callback);
        synchronized(this) {
            callbacks.put(callback, from);
        }
        LOG.exit();
    }
    
    void cancel(Consumer<MessageIterator> callback) {
        LOG.entry(callback);
        synchronized(this) {
           callbacks.remove(callback);
        }
        LOG.exit();
    }
    
    void callback(Function<Instant, MessageIterator> search) {
        HashMap<Consumer<MessageIterator>,Instant> invocable;
        LOG.entry(search);
        synchronized(this) {
            invocable = callbacks;
            callbacks = new HashMap<>();            
        }
        LOG.debug("after synchronized block");
        invocable.forEach((callback,from)->{
            LOG.debug("notifiying {} of messages starting {}", callback, from);
            try (MessageIterator results = search.apply(from)) {
                if (results.hasNext())
                    callback.accept(results);
                else {  
                    LOG.warn("New message did not trigger callback waiting for {}", from);
                    synchronized(this) {
                        callbacks.put(callback, from);
                    }
                }
            }
            LOG.debug("done sending messages to {}", callback);
        });
        LOG.exit();
    }
    
}
