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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
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
    
    private static class Indicator {
        public boolean value = false;
    }
    
    public AbstractFeedService(long poolSize, int bucketSize) {
        this.bufferPool = new BufferPool((int)poolSize);
        this.bucketSize = bucketSize; 
    }
    
    private MessageBuffer getOrCreateBuffer(FeedPath path) throws InvalidPath {
        Indicator bufferCreated = new Indicator();
        MessageBuffer result = feeds.computeIfAbsent(path, k-> {
            bufferCreated.value = true;
            return bufferPool.createBuffer(bucketSize);
        });
        if (bufferCreated.value) startBackEndListener(path, result.now());
        return result;
    }
    
    private Optional<MessageBuffer> getBuffer(FeedPath path) {
        return Optional.ofNullable(feeds.get(path));
    }

    @Override
    public void listen(FeedPath path, Instant from, Consumer<MessageIterator> callback) throws InvalidPath {
        LOG.entry(path, from, callback);
        if (path.isEmpty() || path.part.getId().isPresent()) throw LOG.throwing(new InvalidPath(path));
        getOrCreateBuffer(path).getMessagesAfter(from, callback);
        LOG.exit();
    }
    
    @Override
    public void cancelCallback(FeedPath path, Consumer<MessageIterator> callback) throws InvalidPath {
        LOG.entry(path, callback);
        if (path.isEmpty() || path.part.getId().isPresent()) throw LOG.throwing(new InvalidPath(path));
        getBuffer(path).orElseThrow(()->new InvalidPath(path)).cancelCallback(callback);
        LOG.exit();        
    }

    @Override
    public MessageIterator sync(FeedPath path, Instant from) throws InvalidPath {
        LOG.entry(path, from);
        if (path.isEmpty() || path.part.getId().isPresent()) throw new InvalidPath(path);
        Optional<MessageBuffer> buffer = getBuffer(path);
        if (buffer.isPresent()) {
            MessageIterator buffered = buffer.get().getMessagesAfter(from);
            if (buffer.get().firstTimestamp().map(ts->ts.compareTo(from) < 0).orElse(false)) {
                // Easy: buffer has messages earlier than from, so this should be good
                return LOG.exit(buffered);        
            } else {
                if (buffered.hasNext()) {
                    Message first = buffered.next();
                    LOG.debug("For {} buffer has messages from {}", path, first);
                    return LOG.exit(MessageIterator.of(MessageIterator.of(first), buffered, syncFromBackEnd(path, from, first.getTimestamp())));
                } else {
                    return LOG.exit(syncFromBackEnd(path, from, Instant.MAX));
                }
            }
        } else {
            return LOG.exit(syncFromBackEnd(path, from, Instant.MAX));
        }
    }
    
    @Override
    public Message post(FeedPath path, Message message) throws InvalidPath {
        LOG.entry(path, message);
        if (path.isEmpty() || path.part.getId().isPresent()) throw new InvalidPath(path);
        MessageBuffer buffer = getOrCreateBuffer(path);
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
