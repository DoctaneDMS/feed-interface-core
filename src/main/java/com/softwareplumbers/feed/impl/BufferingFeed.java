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
import com.softwareplumbers.feed.Filters;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.Message.RemoteInfo;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.MessageType;
import com.softwareplumbers.feed.impl.buffer.MessageBuffer;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/** Generic implementation of a Feed where messages which stores messages in memory.
 *
 * @author Jonathan Essex. 
 */
public class BufferingFeed implements Feed {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(BufferingFeed.class);
    
    private static class Callback {
        public final CompletableFuture<MessageIterator> future;
        public final Predicate<Message> predicate;
        
        public Callback(Predicate<Message>... predicates) {
            if (predicates.length == 0) {
                this.predicate = message->true;
            } else if (predicates.length == 1) {
                this.predicate = predicates[0];
            } else {
                this.predicate = Stream.of(predicates).reduce(message->true,  Predicate::and);                    
            }
            this.future = new CompletableFuture<>();
        }
    }
       
    private final Optional<BufferingFeed> parentFeed;
    private final Optional<String> name;
    private final NavigableMap<Instant, List<Callback>> callbacks = new TreeMap<>();
    private final Map<String, BufferingFeed> children = new ConcurrentHashMap<>();
    private final MessageBuffer buffer;
    
    private BufferingFeed(MessageBuffer buffer, BufferingFeed parentFeed, String name) {
        this.parentFeed = Optional.of(parentFeed);
        this.name = Optional.of(name);
        this.buffer = buffer;
    }
    
    private static BufferingFeedService cast(FeedService service) {
        try {
            return (BufferingFeedService)service;
        } catch (ClassCastException e) {
            throw new RuntimeException("Can only use AbstractFeed with AbstractFeedService", e);
        }
    }
    
    public BufferingFeed(MessageBuffer buffer) {
        this.parentFeed = Optional.empty();
        this.name = Optional.empty(); 
        this.buffer = buffer;
    }
    
    public BufferingFeed getFeed(FeedService service, FeedPath path) throws InvalidPath {
        LOG.entry(getName(), service, path);
        if (path.isEmpty()) return this;
        BufferingFeed parent = getFeed(service, path.parent);
        return LOG.exit(path.part.getName()
                .map(name->parent.children.computeIfAbsent(name, key -> new BufferingFeed(cast(service).createBuffer(), parent, key)))
                .orElseThrow(()->LOG.throwing(new InvalidPath(path)))
        );
    }
              
    private void trigger(BufferingFeedService service, Message message) {
        LOG.entry(service, message);
        synchronized(this) {
            Iterator<Map.Entry<Instant, List<Callback>>> activated = callbacks.headMap(message.getTimestamp(), false).entrySet().iterator();
            while (activated.hasNext()) {
                final Map.Entry<Instant, List<Callback>> entry = activated.next();
                Instant entryTimestamp = entry.getKey();
                LOG.trace("Processing callbacks looking for messages after {}", entryTimestamp);
                activated.remove();
                for (Callback callback : entry.getValue()) {
                    if (!callback.future.isCancelled()) {
                        if (callback.predicate.test(message)) {
                            service.callback(() -> { 
                                MessageIterator messages = search(service, entryTimestamp, service.getServerId(), false, callback.predicate);
                                if (messages.hasNext()) {
                                    LOG.trace("Completing callback with messages");
                                    callback.future.complete(messages);
                                } else {
                                    // This shouldn't happen, but... belt and braces.
                                    LOG.warn("No messages, resubmitting callback {}", callback);
                                    synchronized(this) {
                                        callbacks.computeIfAbsent(entryTimestamp, key -> new LinkedList()).add(callback);
                                    }
                                }
                            });
                        } else {
                            LOG.trace("Callback matched no messages");
                            callbacks.computeIfAbsent(entryTimestamp, key -> new LinkedList()).add(callback);
                        }
                    }
                }
            }
        }
        parentFeed.ifPresent(feed->feed.trigger(service, message));
        LOG.exit();
    }

    @Override
    public FeedPath getName() {
        if (!parentFeed.isPresent()) return FeedPath.ROOT;
        return parentFeed.get().getName().add(name.orElseThrow(()->new RuntimeException("missing name")));
    }

    @Override
    public CompletableFuture<MessageIterator> listen(FeedService service, Instant from, UUID serverId, Predicate<Message>... filters) {
        LOG.entry(getName(), service, from, serverId, filters);
        MessageIterator results = search(service, from, serverId, true, filters); 
        if (results.hasNext()) {
            LOG.debug("Found results, returning immediately");
            return LOG.exit(CompletableFuture.completedFuture(results));
        } else {
            Callback result = new Callback(filters);
            synchronized(this) {
                callbacks.computeIfAbsent(from, key -> new LinkedList()).add(result);
            }
            return LOG.exit(result.future);            
        }
    }

    public CompletableFuture<MessageIterator> watch(BufferingFeedService service, Instant from) {
        LOG.entry(getName(), service, from);
        MessageIterator results = search(service, from, service.getServerId(), false, Filters.POSTED_LOCALLY); 
        if (results.hasNext()) {
            LOG.debug("Found results, returning immediately");
            return LOG.exit(CompletableFuture.completedFuture(results));
        } else {
            Callback result = new Callback(Filters.POSTED_LOCALLY);
            synchronized(this) {
                callbacks.computeIfAbsent(from, key -> new LinkedList()).add(result);
            }
            return LOG.exit(result.future);            
        }
    }
    
    /** Post a message.
     * 
     * The given message is submitted to the local message buffer for this feed. Any name, timestamp, or
     * serverId present in the message will be ignored and replaced with the name of this feed plus
     * a generated Id, the current timestamp from the service's clock, and the server Id from the
     * supplied service.
     * 
     * The returned message is an Ack which contains the generated message name, timestamp, and serverId.
     * 
     * @param service
     * @param message
     * @return 
     */
    @Override
    public Message post(FeedService service, Message message) {
        LOG.entry(getName(), service, message);
        BufferingFeedService svc = cast(service);
        message = message
            .setName(getName().addId(svc.generateMessageId()))
            .setServerId(service.getServerId());
        
        Message[] results = buffer.addMessages(message, MessageImpl.acknowledgement(message));
        for (Message result: results) trigger(svc,result);
        return LOG.exit(results[1]);
    }
    
    /** Handle a message replicated from another cluster node.
     * 
     * The given message is submitted to the local message buffer for this feed. The message must contain
     * a message Id, server Id, and timestamp - these will have been generated by the originating server.
     * The serverId and timestamp will be replaced with generated values per the post method, but the original
     * values will be preserved in the 'remote' header.
     * 
     * The returned message is an Ack which contains the local serverId and timestamp.
     * 
     * @param service
     * @param message
     * @return 
     */
    public Message replicate(BufferingFeedService service, Message message) {
        LOG.entry(getName(), service, message);
        message = message.localizeTimestamp(service.getServerId(), null);
        if (message.getType() == MessageType.ACK) {
            Message result = buffer.addMessage(message);
            trigger(service, result);
            return result;
        } else {
            Message[] results = buffer.addMessages(message, MessageImpl.acknowledgement(message));
            for (Message result: results) trigger(service, result);
            return LOG.exit(results[1]);
        }
    }
        
    @Override
    public MessageIterator search(FeedService service, Instant from, UUID serverId, boolean relay, Predicate<Message>... filters) {
        LOG.entry(getName(), service, from, serverId, relay, filters);
        if (children.isEmpty()) {
            return LOG.exit(search(service, from, false, Instant.MAX, true, serverId, relay, filters));
        } else {
            return LOG.exit(search(service, from, false, checkpoint(), true, serverId, relay, filters));
        }
    }
    
    private Instant checkpoint() {
        return Stream.concat(Stream.of(
                buffer.checkpoint()), 
                children.values().stream().map(BufferingFeed::checkpoint)
            ).min(Comparator.naturalOrder())
            .orElseThrow(()->new RuntimeException("Failed checkpoint"));
    }
    
    @Override
    public MessageIterator search(FeedService service, String id, Predicate<Message>... filter) {
        LOG.entry(getName(), service, id, filter);
        return LOG.exit(buffer.getMessages(id, filter));
    }
    
    /** Return a filter that will find a message based on its timestamp on some other node.
     * 
     * @param service The feed service (i.e. the local node)
     * @param from Lower bound for timestamp (exclusive)
     * @param to Upper bound for timestamp (inclusive)
     * @param serverId Id of remote node
     * @return A predicate object implementing the specified filter
     */
    private Predicate<Message> filterByServerPerspective(BufferingFeedService service, Instant from, Instant to, UUID serverId) {
        // Filter based on the time a message was received on some other server
        final Instant initTime = service.getCluster().getService(serverId)
            .orElseThrow(()->new RuntimeException("invalid server id " + serverId))
            .getInitTime();
        
        return message -> {
            Instant timestamp = message.getTimestamp();
            if (message.getRemoteInfo().isPresent()) {
                // it's a message from a remote
                RemoteInfo remoteInfo = message.getRemoteInfo().get();
                if (remoteInfo.serverId.equals(serverId)) {
                    // the only remote messages we need to futz with are the ones sent by the specified serverId;
                    // in this case we use the remote timestamp already in the message
                    timestamp = remoteInfo.timestamp;
                }
            } else {
                // its a message that was posted locally. In this case we just need to switch the timestamp with
                // the one that was reported in the Ack from the specified server.
                timestamp = buffer.getMessages(message.getId(), Filters.IS_ACK, Filters.fromRemote(serverId))
                   .toStream()
                   .findAny() // Find any ACK from the specified server 
                   .map(Message::getTimestamp)
                   .orElse(timestamp.isBefore(initTime) ? timestamp : initTime); // Haven't received the ack yet, or this is a request from a new node
            }
            return timestamp.isAfter(from) && !to.isBefore(timestamp);
        };
    }
    
    @Override
    public MessageIterator search(FeedService svc, Instant from, boolean fromInclusive, Instant to, boolean toInclusive, UUID serverId, boolean relay, Predicate<Message>... filters) {
        LOG.entry(getName(), svc, from, fromInclusive, to, toInclusive, serverId, relay, filters);
        MessageIterator result;
        boolean bufferedDataComplete;
        BufferingFeedService service = cast(svc);
        
        // Fetch any locally buffered data
        if (serverId == null || serverId.equals(service.getServerId())) {
            result = buffer.getMessagesBetween(from, fromInclusive, to, toInclusive, filters);
            LOG.debug("First message in buffer at {}", buffer.firstTimestamp());
            bufferedDataComplete = buffer.firstTimestamp().map(first->!first.isAfter(from)).orElse(false);
            LOG.debug("Buffered data considered complete: {}", bufferedDataComplete);
        } else {
            Instant acksFrom = from.minusSeconds(service.getAckTimeout()); // Add extra time to ensure we fetch all the acks
            Predicate<Message> filter = Stream.of(filters).reduce(message->true,  Predicate::and);        
            result = buffer.getMessagesAfter(acksFrom, filterByServerPerspective(service, from, to, serverId).and(filter));
            bufferedDataComplete = buffer.firstTimestamp().map(first->!first.isAfter(acksFrom)).orElse(false);
        }
        
        
        if (!bufferedDataComplete && relay) {
            try {
                if (result.hasNext()) {
                    Message first = result.next();
                    result = MessageIterator.of(relay(service, from, fromInclusive, first.getTimestamp(), false, serverId, filters), MessageIterator.of(first), result);
                } else {
                    result = relay(service, from, fromInclusive, to, toInclusive, serverId, filters);
                }
            } catch (InvalidPath exp) {
                // Invalid path shouldn't happen here
                throw LOG.throwing(FeedExceptions.runtime(exp));
            }
        }
        
        if (!children.isEmpty()) {
            Stream<MessageIterator> feeds = Stream.concat(
                Stream.of(result), 
                children.values().stream().map(feed->feed.search(service, from, fromInclusive, to, toInclusive, serverId, relay, filters)));
            result = MessageIterator.merge(feeds);            
        }

        if (LOG.isTraceEnabled()) {
            if (result.hasNext()) {
                result = result.peekable();
                LOG.trace("search returning results starting from {}", ((MessageIterator.Peekable)result).peek().get().getTimestamp());
            } else {
                LOG.trace("search returns no results");
            }
        }
        return LOG.exit(result);
    }
    
    public void dumpState(PrintWriter out) {
        out.write("Feed: ");
        out.write(getName().toString());
        out.write("\n");
        out.write("Outstanding callbacks: ");
        out.write(Integer.toString(callbacks.size()));
        out.write("\n");
        buffer.dumpState(out);
    }
    
    public Stream<BufferingFeed> getLiveFeeds() {        
        Stream<BufferingFeed> result = children.values().stream().flatMap(BufferingFeed::getLiveFeeds);
        return buffer.isEmpty() ? result : Stream.concat(Stream.of(this), result);
    }
    
    public MessageIterator relay(BufferingFeedService service, Instant from, boolean fromInclusive, Instant to, boolean toInclusive, UUID serverId, Predicate<Message>... filters) throws InvalidPath {
        LOG.entry(getName(), service, from, fromInclusive, to, toInclusive, serverId, filters);
        MessageIterator result = MessageIterator.EMPTY;
        try (Stream<FeedService> remotes = service.getCluster().getServices(remote -> !Objects.equals(service.getServerId(), remote.getServerId()))) {
            for (FeedService remote : (Iterable<FeedService>)remotes::iterator) {
                MessageIterator.Peekable remoteResult = remote.search(getName(), from, fromInclusive, to, toInclusive, serverId, false, filters).peekable();
                Instant remoteFrom = remoteResult.peek().map(message->message.getTimestamp()).orElse(Instant.MAX);
                LOG.debug("Relay search found messages between {} and {} on serverId {}", from, to, remote.getServerId());
                if (toInclusive && !remoteFrom.isAfter(to) || remoteFrom.isBefore(to)) {
                    result = MessageIterator.of(remoteResult, result);
                    to = remoteFrom;
                    toInclusive = false;
                }
            }
        }
        return LOG.exit(result);
    }  
    

}
