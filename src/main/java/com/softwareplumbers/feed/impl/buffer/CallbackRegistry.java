/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import com.softwareplumbers.feed.MessageIterator;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
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
    
    LinkedList<Entry> callbacks = new LinkedList<>();
    
    public void addCallback(Instant from, Consumer<MessageIterator> callback) {
        LOG.entry(from, callback);
        synchronized(this) {
            callbacks.add(new Entry(from, callback));
        }
        LOG.exit();
    }
    
    void callback(Function<Instant, MessageIterator> search) {
        List<Entry> invocable;
        LOG.entry(search);
        synchronized(this) {
            invocable = callbacks;
            callbacks = new LinkedList<>();            
        }
        LOG.debug("after synchronized block");
        invocable.stream().forEach(entry->{
            LOG.debug("notifiying {} of messages starting {}", entry.callback, entry.from);
            try (MessageIterator results = search.apply(entry.from)) {
                entry.callback.accept(results);
            }
            LOG.debug("done sending messages to {}", entry.callback);
        });
        LOG.exit();
    }
    
}
