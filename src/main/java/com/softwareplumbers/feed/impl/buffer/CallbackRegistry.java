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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/**
 *
 * @author jonathan
 */
public class CallbackRegistry {
    
    private static XLogger LOG = XLoggerFactory.getXLogger(CallbackRegistry.class);
    
    private ExecutorService executor;
    
    private NavigableMap<Instant, List<CompletableFuture<MessageIterator>>> callbacks = new TreeMap<>();
    
    public CallbackRegistry(ExecutorService executor) {
        this.executor = executor;
    }
    
    public CompletableFuture<MessageIterator> submitSearch(Instant from) {
        LOG.entry(from);
        CompletableFuture<MessageIterator> result = new CompletableFuture<>();
        synchronized(this) {
            callbacks.computeIfAbsent(from, key -> new LinkedList()).add(result);
        }
        return LOG.exit(result);
    }
    
    void notify(Instant timestamp, Function<Instant, MessageIterator> search) {
        LOG.entry(timestamp, search);
        synchronized(this) {
            Iterator<Map.Entry<Instant, List<CompletableFuture<MessageIterator>>>> activated = callbacks.headMap(timestamp, false).entrySet().iterator();
            while (activated.hasNext()) {
                final Map.Entry<Instant, List<CompletableFuture<MessageIterator>>> entry = activated.next();
                Instant entryTimestamp = entry.getKey();
                activated.remove();
                for (CompletableFuture<MessageIterator> result : entry.getValue()) {
                    if (!result.isCancelled()) {
                        executor.submit(() -> { 
                            MessageIterator messages = search.apply(entryTimestamp);
                            if (messages.hasNext()) {
                                result.complete(messages);
                            } else {
                                LOG.warn("No messages, resubmitting callback {}", result);
                                synchronized(this) {
                                    callbacks.computeIfAbsent(entryTimestamp, key -> new LinkedList()).add(result);
                                }
                            }
                        });
                    }
                }
            }
        }
        LOG.exit();
    }
    
}
