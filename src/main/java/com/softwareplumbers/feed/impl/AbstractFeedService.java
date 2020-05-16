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
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/**
 *
 * @author jonathan
 */
public abstract class AbstractFeedService implements FeedService {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(AbstractFeedService.class);
    
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
        LOG.entry(path, from, callback);
        if (path.isEmpty() || path.part.getId().isPresent()) throw LOG.throwing(new InvalidPath(path));
        getBuffer(path).getMessagesAfter(from, callback);
        LOG.exit();
    }

    @Override
    public MessageIterator sync(FeedPath path, Instant from) throws InvalidPath {
        LOG.entry(path, from);
        if (path.isEmpty() || path.part.getId().isPresent()) throw new InvalidPath(path);
        MessageBuffer buffer = getBuffer(path);
        MessageIterator buffered = buffer.getMessagesAfter(from);
        if (buffer.firstTimestamp().map(ts->ts.compareTo(from) < 0).orElse(false)) {
            // Easy: buffer has messages earlier than from, so this should be good
            return LOG.exit(buffer.getMessagesAfter(from));        
        } else {
            if (buffered.hasNext()) {
                Message first = buffered.next();
                LOG.debug("For {} buffer has messages from {}", path, first);
                return LOG.exit(MessageIterator.of(MessageIterator.of(first), buffered, syncFromBackEnd(path, from, first.getTimestamp())));
            } else {
                return LOG.exit(syncFromBackEnd(path, from, Instant.MAX));
            }
        }      
    }
    
    @Override
    public Message post(FeedPath path, Message message) throws InvalidPath {
        LOG.entry(path, message);
        if (path.isEmpty() || path.part.getId().isPresent()) throw new InvalidPath(path);
        MessageBuffer buffer = getBuffer(path);
        synchronized(buffer) {
            if (buffer.isEmpty()) {
                startBackEndListener(path, buffer.now());
            } 
        }
        return LOG.exit(buffer.addMessage(message));
    }
    
    public void dumpState() {
        for (Map.Entry<FeedPath,MessageBuffer> entry : feeds.entrySet()) {
            System.out.println("feed: " + entry.getKey());
            entry.getValue().dumpBuffer();
        }
    }
    
    protected abstract MessageIterator syncFromBackEnd(FeedPath path, Instant from, Instant to) throws InvalidPath;   
    protected abstract void startBackEndListener(FeedPath path, Instant from) throws InvalidPath;
}
