/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.Feed;
import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.FeedService;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.impl.buffer.BufferPool;
import com.softwareplumbers.feed.impl.buffer.MessageBuffer;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 *
 * @author jonathan
 */
public abstract class AbstractFeedService implements FeedService {
    
    private final BufferPool bufferPool;
    private final Map<FeedPath, MessageBuffer> feeds = new ConcurrentHashMap<>();
    private final int bucketSize;
    
    public AbstractFeedService(long poolSize, int bucketSize) {
        this.bufferPool = new BufferPool((int)poolSize);
        this.bucketSize = bucketSize; 
    }
    
    private MessageBuffer getBuffer(FeedPath path) {
        return feeds.computeIfAbsent(path, p->bufferPool.createBuffer(bucketSize));
    }

    @Override
    public void listen(FeedPath path, Instant from, Consumer<MessageIterator> callback) throws InvalidPath {
        if (path.isEmpty() || path.part.getId().isPresent()) throw new InvalidPath(path);
        getBuffer(path).getMessagesAfter(from, callback);
    }

    @Override
    public MessageIterator sync(FeedPath path, Instant from) throws InvalidPath {
        if (path.isEmpty() || path.part.getId().isPresent()) throw new InvalidPath(path);
        MessageBuffer buffer = getBuffer(path);
        MessageIterator buffered = buffer.getMessagesAfter(from);
        if (buffer.firstTimestamp().map(ts->ts.compareTo(from) < 0).orElse(false)) {
            // Easy: buffer has messages earlier than from, so this should be good
            return buffer.getMessagesAfter(from);        
        } else {
            if (buffered.hasNext()) {
                Message first = buffered.next();
                System.out.println("FIRST:--" + first);
                return MessageIterator.of(MessageIterator.of(first), buffered, syncFromBackEnd(path, from, first.getTimestamp()));
            } else {
                return syncFromBackEnd(path, from, Instant.MAX);
            }
        }      
    }
    
    @Override
    public Message post(FeedPath path, Message message) throws InvalidPath {
        if (path.isEmpty() || path.part.getId().isPresent()) throw new InvalidPath(path);
        MessageBuffer buffer = getBuffer(path);
        synchronized(buffer) {
            if (buffer.isEmpty()) {
                startBackEndListener(path, buffer.now());
            } 
        }
        return buffer.addMessage(message);
    }
    
    public void dumpState() {
        for (Map.Entry<FeedPath,MessageBuffer> entry : feeds.entrySet()) {
            System.out.println("feed: " + entry.getKey());
            entry.getValue().dumpBuffer();
        }
    }
    
    protected abstract MessageIterator syncFromBackEnd(FeedPath path, Instant from, Instant to);   
    protected abstract void startBackEndListener(FeedPath path, Instant from);
}
