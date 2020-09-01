/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.FeedService;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.impl.buffer.BufferPool;
import com.softwareplumbers.feed.impl.buffer.MessageBuffer;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/** Implements a basic feed service on top of some other data store.
 * 
 * Uses the MessageBuffer class to implement a circular buffer for messages;
 * abstract methods must be implemented to connect this buffer to a back end
 * message store.
 *
 * @author jonathan
 */
public abstract class AbstractFeedService implements FeedService {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(AbstractFeedService.class);
    
    private final BufferPool bufferPool;
    private final Map<FeedPath, MessageBuffer> feeds = new ConcurrentHashMap<>();
    private final int bucketSize;
    private final UUID serverId = UUID.randomUUID();
    
    private static class Indicator {
        public boolean value = false;
    }
    
    /** Create a new Abstract Feed Service.
     * 
     * @param callbackExecutor Executor service for running callbacks
     * @param poolSize Amount of memory in bytes to dedicate to all message buffers
     * @param bucketSize Size of a bucket - should be larger than the expected maximum message size
     */
    public AbstractFeedService(ExecutorService callbackExecutor, long poolSize, int bucketSize) {
        this.bufferPool = new BufferPool(callbackExecutor, (int)poolSize);
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
    public CompletableFuture<MessageIterator> listen(FeedPath path, Instant from) throws InvalidPath {
        LOG.entry(path, from);
        if (path.isEmpty() || path.part.getId().isPresent()) throw LOG.throwing(new InvalidPath(path));
        return LOG.exit(getOrCreateBuffer(path).getFutureMessagesAfter(from));
    }
    
    @Override
    public MessageIterator sync(FeedPath path, Instant from, UUID serverId) throws InvalidPath {
        LOG.entry(path, from, serverId);
        if (path.isEmpty() || path.part.getId().isPresent()) throw new InvalidPath(path);
        if (serverId == null || serverId.equals(this.serverId)) {
            // Request to sync from client previosuly subscribed to this instance
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
                        return LOG.exit(MessageIterator.of(MessageIterator.of(first), buffered, syncFromBackEnd(path, from, first.getTimestamp(), serverId)));
                    } else {
                        return LOG.exit(syncFromBackEnd(path, from, Instant.MAX, serverId));
                    }
                }
            } else {
                return LOG.exit(syncFromBackEnd(path, from, Instant.MAX, serverId));
            }
        } else {
            return LOG.exit(syncFromBackEnd(path, from, Instant.MAX, serverId));
        }
    }
    
    @Override
    public Message post(FeedPath path, Message message) throws InvalidPath {
        LOG.entry(path, message);
        if (path.isEmpty() || path.part.getId().isPresent()) throw new InvalidPath(path);
        message = message.setName(path.addId(getIdFromBackEnd()));
        MessageBuffer buffer = getOrCreateBuffer(path);
        return LOG.exit(buffer.addMessage(message));
    }
    
    public UUID getServerId() {
        return serverId;
    }
    
    public void dumpState() {
        for (Map.Entry<FeedPath,MessageBuffer> entry : feeds.entrySet()) {
            System.out.println("feed: " + entry.getKey());
            entry.getValue().dumpBuffer();
        }
    }
    
    protected abstract String getIdFromBackEnd();
    protected abstract MessageIterator syncFromBackEnd(FeedPath path, Instant from, Instant to, UUID serverId) throws InvalidPath;   
    protected abstract void startBackEndListener(FeedPath path, Instant from) throws InvalidPath;
}
