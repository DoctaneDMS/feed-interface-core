/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import com.softwareplumbers.feed.Message;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 *
 * @author jonathan
 */
public class CallbackRegistry {
    
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
        synchronized(this) {
            callbacks.add(new Entry(from, callback));
        }
    }
    
    void callback(Function<Instant, MessageIterator> search) {
        List<Entry> invocable;
        synchronized(this) {
            invocable = callbacks;
            callbacks = new LinkedList<>();            
        }
        invocable.stream().forEach(entry->{
            try (MessageIterator results = search.apply(entry.from)) {
                entry.callback.accept(results);
            }
        });
    }
    
}
