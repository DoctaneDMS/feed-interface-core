/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.Message;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 * @author jonathan
 */
public abstract class MessageIterator implements Closeable, Iterator<Message> {

    private final Runnable closeHandler;
    
    private MessageIterator(Runnable closeHandler) {
        this.closeHandler = closeHandler;
    }
    
    @Override
    public void close() {
        closeHandler.run();
    }

    public Stream<Message> toStream() {
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                this,
                Spliterator.ORDERED
            ), 
            false);        
    }
    
    public MessageIterator peek(Consumer<Message> consumer) {
        return new Chained(this, consumer);
    }

    private static class Delegator extends MessageIterator {
        private final Iterator<Message> base;
    
        public Delegator(Iterator<Message> base, Runnable closeHandler) {
            
            super(closeHandler);
            this.base = base;
        }

        @Override
        public boolean hasNext() {
            return base.hasNext();
        }

        @Override
        public Message next() {
            return base.next();
        }  
    }

    private static class Chained extends Delegator {
        
        private final Consumer<Message> consumer;
    
        public Chained(MessageIterator base, Consumer<Message> consumer) {
            super(base, base.closeHandler);
            this.consumer = consumer;
        }

        @Override
        public Message next() {
            Message result = super.next();
            consumer.accept(result);
            return result;
        }  
    }
    
    private static class Singleton extends MessageIterator {
        private Message message;
        
        public Singleton(Message message) {
            super(()->{});
        }
        
        @Override
        public boolean hasNext() {
            return message != null;
        }

        @Override
        public Message next() {
            Message result = message;
            message = null;
            return result;
        }  
    }
    
    public static MessageIterator of(Iterator<Message> messages, Runnable closeHandler) {
        return new Delegator(messages, closeHandler);
    }
    
    public static MessageIterator of(Stream<Message> messages) {
        return new Delegator(messages.iterator(), ()->messages.close());
    }
    
    public static MessageIterator of(Message message) {
        return new Singleton(message);
    }
        
}
