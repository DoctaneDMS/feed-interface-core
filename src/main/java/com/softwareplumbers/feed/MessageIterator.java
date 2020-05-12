/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import com.softwareplumbers.feed.Message;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 * @author jonathan
 */
public class MessageIterator implements Closeable, Iterator<Message> {
    
    private final Runnable closeHandler;
    private final Iterator<Message> base;
    
    public MessageIterator(Iterator<Message> base, Runnable closeHandler) {
        this.base = base;
        this.closeHandler = closeHandler;
    }
    
    public MessageIterator(Message item) {
        this(Collections.singleton(item).iterator(), ()->{});
    }

    @Override
    public void close() {
        closeHandler.run();
    }

    @Override
    public boolean hasNext() {
        return base.hasNext();
    }

    @Override
    public Message next() {
        return base.next();
    }   
    
    public Stream<Message> toStream() {
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                this,
                Spliterator.ORDERED
            ), 
            false);        
    }
}
