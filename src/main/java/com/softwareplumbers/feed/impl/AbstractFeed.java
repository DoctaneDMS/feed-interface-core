/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.Feed;
import com.softwareplumbers.feed.FeedExceptions;
import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.FeedService;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.MessageType;
import com.softwareplumbers.feed.impl.buffer.MessageBuffer;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/**
 *
 * @author jonathan
 */
public class AbstractFeed implements Feed {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(AbstractFeed.class);
    
    private final AbstractFeedService service;
    private final Optional<AbstractFeed> parentFeed;
    private final Optional<String> name;
    private final NavigableMap<Instant, List<CompletableFuture<MessageIterator>>> callbacks = new TreeMap<>();
    private final Map<String, AbstractFeed> children = new ConcurrentHashMap<>();
    private final MessageBuffer buffer;
    
    private AbstractFeed(AbstractFeedService service, AbstractFeed parentFeed, String name) {
        this.service = service;
        this.parentFeed = Optional.of(parentFeed);
        this.name = Optional.of(name);
        this.buffer = service.createBuffer();
    }
    
    public AbstractFeed(AbstractFeedService service) {
        this.service = service;
        this.parentFeed = Optional.empty();
        this.name = Optional.empty();
        this.buffer = service.createBuffer();
    }
    
    public AbstractFeed getFeed(FeedPath path) throws InvalidPath {
        if (path.isEmpty()) return this;
        AbstractFeed parent = getFeed(path.parent);
        return path.part.getName()
            .map(name->parent.children.computeIfAbsent(name, key -> new AbstractFeed(service, parent, key)))
            .orElseThrow(()->LOG.throwing(new InvalidPath(path)));
    }
              
    protected void trigger(Message message) {
        LOG.entry(message);
        synchronized(this) {
            Iterator<Map.Entry<Instant, List<CompletableFuture<MessageIterator>>>> activated = callbacks.headMap(message.getTimestamp(), false).entrySet().iterator();
            while (activated.hasNext()) {
                final Map.Entry<Instant, List<CompletableFuture<MessageIterator>>> entry = activated.next();
                Instant entryTimestamp = entry.getKey();
                activated.remove();
                for (CompletableFuture<MessageIterator> result : entry.getValue()) {
                    if (!result.isCancelled()) {
                        service.callback(() -> { 
                            MessageIterator messages = search(entryTimestamp);
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
        parentFeed.ifPresent(feed->feed.trigger(message));
        LOG.exit();
    }

    @Override
    public FeedPath getName() {
        if (!parentFeed.isPresent()) return FeedPath.ROOT;
        return parentFeed.get().getName().add(name.orElseThrow(()->new RuntimeException("missing name")));
    }

    @Override
    public CompletableFuture<MessageIterator> listen(FeedService service, Instant from) {
        LOG.entry(service, from);
        MessageIterator results = search(from);
        if (results.hasNext()) {
            LOG.debug("Found results, returning immediately");
            return LOG.exit(CompletableFuture.completedFuture(results));
        } else {
            CompletableFuture<MessageIterator> result = new CompletableFuture<>();
            synchronized(this) {
                callbacks.computeIfAbsent(from, key -> new LinkedList()).add(result);
            }
            return LOG.exit(result);            
        }
    }
    
    public Message post(Message message) {
        message = message.setName(getName().addId(service.generateMessageId()));
        Message result = buffer.addMessage(message);
        trigger(result);
        return result;
    }
    
    public MessageIterator search(Instant from, Instant to) {
        Stream<MessageIterator> feeds = Stream.concat(
            Stream.of(buffer.getMessagesBetween(from, false, to, true)), 
            children.values().stream().map(feed->feed.search(from,to)));
        return MessageIterator.merge(feeds);
    }
    
    public MessageIterator search(Instant from) {
        if (children.isEmpty()) {
            return buffer.getMessagesAfter(from);
        } else {
            // We do this to ensure we are returning all the messages there will ever be
            // with a timestamp less than now. 
            Instant now = Instant.now(service.getClock());
            return search(from, now);
        }
    }
    
    public MessageIterator getMessages(String id, Predicate<Message>... filter) {
        return buffer.getMessages(id, filter);
    }
    
    private Predicate<Message> filterByServerPerspective(Instant from, Instant to, UUID serverId) {
        // Transform messages timestamp another server's perspective. For each message, we should
        // have an ack saying when it arrived on that server. So the message timestamp becomes the
        // ack timestamp, while the ack timestamp for messages originally posted to this server becomes
        // the message timestamp
        return message -> {
            Instant timestamp = message.getTimestamp();
            if (message.getType() == MessageType.ACK) {
                // This message is an ACK
                if (message.getServerId().equals(service.getServerId())) {
                    timestamp = getMessages(message.getId(), m -> m.getType() != MessageType.ACK) // Find the corresponding non-ack
                        .toStream()
                        .findAny()
                        .map(Message::getTimestamp) // Get the timestamp of the main message
                        .orElse(timestamp);
                } 
            } else {
                // This message is not an ACK
                timestamp = getMessages(message.getId(), m -> m.getType() == MessageType.ACK && m.getServerId().equals(serverId)) // Find the ACK from the requested server
                    .toStream()
                    .findAny()
                    .map(Message::getTimestamp) // Get the timestamp of the ack
                    .orElse(timestamp);
            }
            return timestamp.isAfter(from) && !to.isBefore(timestamp);
        };
    }
    
    public MessageIterator sync(Instant from, boolean fromInclusive, Instant to, boolean toInclusive, UUID serverId) {
        LOG.entry(from, serverId);
        MessageIterator result;
        boolean bufferedDataComplete;
        
        // Fetch any locally bufffered data
        if (serverId == null || serverId.equals(service.getServerId())) {
            result = buffer.getMessagesBetween(from, fromInclusive, to, toInclusive);
            bufferedDataComplete = buffer.firstTimestamp().map(first->!first.isAfter(from)).orElse(false);
        } else {
            Instant acksFrom = from.minusSeconds(service.getAckTimeout()); // Add extra time to ensure we fetch all the acks
            result = MessageIterator.of(buffer.getMessagesAfter(acksFrom).toStream().filter(filterByServerPerspective(from, to, serverId)));
            bufferedDataComplete = buffer.firstTimestamp().map(first->!first.isAfter(acksFrom)).orElse(false);
        }
        
        
        if (!bufferedDataComplete) {
            try {
                if (result.hasNext()) {
                    Message first = result.next();
                    result = MessageIterator.of(service.syncFromBackEnd(getName(), from, fromInclusive, first.getTimestamp(), false, serverId), MessageIterator.of(first), result);
                } else {
                    result = service.syncFromBackEnd(getName(), from, fromInclusive, to, toInclusive, serverId);
                }
            } catch (InvalidPath exp) {
                // Invalid path shouldn't happen here
                throw FeedExceptions.runtime(exp);
            }
        }
        
        if (!children.isEmpty()) {
            Stream<MessageIterator> feeds = Stream.concat(
                Stream.of(result), 
                children.values().stream().map(feed->feed.sync(from, fromInclusive, to, toInclusive, serverId)));
            MessageIterator.merge(feeds);            
        }

        return LOG.exit(result);
    }
    
    public void dumpState(PrintWriter out) throws IOException {
        out.write("Feed: ");
        out.write(getName().toString());
        out.write("\n");
        out.write("Outstanding callbacks: ");
        out.write(Integer.toString(callbacks.size()));
        out.write("\n");
        buffer.dumpState(out);
    }
    
    public Stream<AbstractFeed> getLiveFeeds() {        
        Stream<AbstractFeed> result = children.values().stream().flatMap(AbstractFeed::getLiveFeeds);
        return buffer.isEmpty() ? result : Stream.concat(Stream.of(this), result);
    }
}
