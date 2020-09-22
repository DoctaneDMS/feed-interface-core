/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.Feed;
import com.softwareplumbers.feed.FeedExceptions;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.FeedService;
import com.softwareplumbers.feed.Filters;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.MessageType;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Arrays;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/**
 *
 * @author jonat
 */
public abstract class AbstractFeed implements Feed {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(AbstractFeed.class);
    
    private static class Callback {
        public final CompletableFuture<MessageIterator> future;
        public final Predicate<Message> predicate;
        public final Instant fromTime;
        public final Instant expiry;
        
        public Callback(Instant fromTime, Instant expiry, Predicate<Message>... predicates) {
            if (predicates.length == 0) {
                this.predicate = message->true;
            } else if (predicates.length == 1) {
                this.predicate = predicates[0];
            } else {
                this.predicate = Stream.of(predicates).reduce(message->true,  Predicate::and);                    
            }
            this.future = new CompletableFuture<>();
            this.fromTime = fromTime;
            this.expiry = expiry;
        }
        
        @Override
        public String toString() {
            return String.format("Callback[ fromTime: %s, expiry: %s, predicate: %s]", fromTime, expiry, predicate);
        }
    }
       
    private final Optional<AbstractFeed> parentFeed;
    private final Optional<String> name;
    private final NavigableMap<Instant, List<Callback>> callbacks = new TreeMap<>();
    private final NavigableMap<Instant, List<Callback>> timeouts = new TreeMap<>();
    private final Map<String, AbstractFeed> children = new ConcurrentHashMap<>();   
    private ScheduledFuture<?> nextTimeout;
    
    private synchronized void addCallback(AbstractFeedService service, Callback callback) {
        LOG.entry(callback);
        callbacks.computeIfAbsent(callback.fromTime, key -> new LinkedList()).add(callback);
        if (callback.expiry != null) {
            timeouts.computeIfAbsent(callback.expiry, key->new LinkedList()).add(callback);
            scheduleTimeout(service, Optional.of(callback.expiry));
        }
        LOG.exit();
    }
    
    private synchronized void removeTimeout(Callback callback) {
        LOG.entry(callback);
        if (callback.expiry != null)
            timeouts.compute(callback.expiry, (expiry, bucket)-> {
                bucket.remove(callback);
                return bucket.isEmpty() ? null : bucket;
            });
        LOG.exit();
    }
    
    private synchronized void removeTrigger(Callback callback) {
        LOG.entry(callback);
        callbacks.compute(callback.fromTime, (fromTime, bucket)-> {
            bucket.remove(callback);
            return bucket.isEmpty() ? null : bucket;            
        });
        LOG.exit();
    }

    
    public AbstractFeed() {
        this.parentFeed = Optional.empty();
        this.name = Optional.empty();         
    }
    
    public AbstractFeed(AbstractFeed parentFeed, String name) {
        this.parentFeed = Optional.of(parentFeed);
        this.name = Optional.of(name);
        
    }
    
    public AbstractFeed getFeed(AbstractFeedService service, FeedPath path) throws FeedExceptions.InvalidPath {
        LOG.entry(getName(), service, path);
        if (path.isEmpty()) return this;
        AbstractFeed parent = getFeed(service, path.parent);
        return LOG.exit(path.part.getName()
                .map(name->parent.children.computeIfAbsent(name, key -> service.createFeed(parent, key)))
                .orElseThrow(()->LOG.throwing(new FeedExceptions.InvalidPath(path)))
        );
    }
    
    protected abstract Message[] store(Message... message);
    
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
    @Override
    public Message replicate(FeedService service, Message message) {
        LOG.entry(getName(), service, message);
        message = message.localizeTimestamp(service.getServerId(), null);
        if (message.getType() == MessageType.ACK) {
            Message result = store(message)[0];
            trigger((AbstractFeedService)service, result);
            return result;
        } else {
            Message[] results = store(message, MessageImpl.acknowledgement(message));
            for (Message result: results) trigger((AbstractFeedService)service, result);
            return LOG.exit(results[1]);
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
        AbstractFeedService svc = (AbstractFeedService)service;
        message = message
            .setName(getName().addId(svc.generateMessageId()))
            .setServerId(service.getServerId());
        
        Message[] results = store(message, MessageImpl.acknowledgement(message));
        for (Message result: results) trigger(svc,result);
        return LOG.exit(results[1]);
    }    
              
    protected void trigger(AbstractFeedService service, Message message) {
        LOG.entry(service, message);
        synchronized(this) {
            Iterator<Map.Entry<Instant, List<Callback>>> activated = callbacks.headMap(message.getTimestamp(), false).entrySet().iterator();
            while (activated.hasNext()) {
                final Map.Entry<Instant, List<Callback>> entry = activated.next();
                Instant entryTimestamp = entry.getKey();
                LOG.trace("Processing callbacks looking for messages after {}", entryTimestamp);
                activated.remove();
                for (Callback callback : entry.getValue()) {
                    removeTimeout(callback);
                    if (!callback.future.isCancelled()) {
                        if (callback.predicate.test(message)) {
                            service.callback(() -> { 
                                MessageIterator messages = search(service, service.getServerId(), callback.fromTime, Optional.of(false), callback.predicate);
                                if (messages.hasNext()) {
                                    LOG.trace("Completing callback with messages");
                                    callback.future.complete(messages);
                                } else {
                                    // This shouldn't happen, but... belt and braces.
                                    LOG.warn("No messages, resubmitting callback {}", callback);
                                    messages.close();
                                    addCallback(service, callback);
                                }
                            });
                        } else {
                            LOG.trace("Callback matched no messages");
                            addCallback(service, callback);
                        }
                    }
                }
            }
        }
        parentFeed.ifPresent(feed->feed.trigger(service, message));
        LOG.exit();
    }
    
    protected Optional<Instant> timeout(AbstractFeedService service) {
        LOG.entry(service);
        Instant now = Instant.now();
        synchronized(this) {
            Iterator<Map.Entry<Instant, List<Callback>>> activated = timeouts.headMap(now, false).entrySet().iterator();
            while (activated.hasNext()) {
                final Map.Entry<Instant, List<Callback>> entry = activated.next();
                LOG.trace("Processing timeouts for callbacks after {}", now);
                activated.remove();
                for (Callback callback : entry.getValue()) {
                    removeTrigger(callback);
                    if (!callback.future.isCancelled()) {
                        service.callback(() -> { 
                            MessageIterator messages = search(service, service.getServerId(), callback.fromTime, Optional.of(false), callback.predicate);
                            callback.future.complete(messages);
                        });
                    }
                }
            }
            return LOG.exit(timeouts.isEmpty() ? Optional.empty() : Optional.of(timeouts.firstKey()));
        }
    }

    public void scheduleTimeout(AbstractFeedService service, Optional<Instant> deadline) {
        LOG.entry(service, deadline);
        if (deadline.isPresent()) {
            long deadlineMillis = deadline.get().toEpochMilli() - Instant.now().toEpochMilli();
            if (nextTimeout  == null || deadlineMillis < nextTimeout.getDelay(TimeUnit.MILLISECONDS)) {
                if (nextTimeout != null) nextTimeout.cancel(false);
                service.schedule(
                    ()->{
                        timeout(service);
                        scheduleTimeout(service, timeout(service));
                    }, 
                    deadlineMillis
                );
            }
        }
    }

    @Override
    public FeedPath getName() {
        if (!parentFeed.isPresent()) return FeedPath.ROOT;
        return parentFeed.get().getName().add(name.orElseThrow(()->new RuntimeException("missing name")));
    }

    @Override
    public CompletableFuture<MessageIterator> listen(FeedService service, Instant from, UUID serverId, long timeoutMillis, Predicate<Message>... filters) {
        LOG.entry(getName(), service, from, serverId, timeoutMillis, filters);
        MessageIterator results = search(service, serverId, from, filters); 
        if (results.hasNext()) {
            LOG.debug("Found results, returning immediately");
            return LOG.exit(CompletableFuture.completedFuture(results));
        } else {
            results.close();
            Instant expiry = timeoutMillis > 0 ? Instant.now().plusMillis(timeoutMillis) : null;
            Callback result = new Callback(from, expiry, filters);
            addCallback((AbstractFeedService)service, result);
            return LOG.exit(result.future);            
        }
    }

    public CompletableFuture<MessageIterator> watch(FeedService service, Instant from, long timeoutMillis) {
        LOG.entry(getName(), service, from);
        MessageIterator results = search(service, service.getServerId(), from, Optional.of(false), Filters.POSTED_LOCALLY); 
        if (results.hasNext()) {
            LOG.debug("Found results, returning immediately");
            return LOG.exit(CompletableFuture.completedFuture(results));
        } else {
            results.close();
            Instant expiry = timeoutMillis > 0 ? Instant.now().plusMillis(timeoutMillis) : null;
            Callback result = new Callback(from, expiry, Filters.POSTED_LOCALLY);
            addCallback((AbstractFeedService)service, result);
            return LOG.exit(result.future);            
        }
    }

    public Stream<AbstractFeed> getChildren() {        
        return children.values().stream();
    }
    
    public Stream<AbstractFeed> getDescendents() {
        return children.values().stream().flatMap(child->Stream.concat(Stream.of(child), child.getDescendents()));
    }
    
    public abstract MessageIterator localSearch(FeedService svc, Instant from, boolean fromInclusive, Optional<Instant> to, Optional<Boolean> toInclusive, Predicate<Message>... filters);
    
    public abstract boolean hasCompleteData(FeedService svc, Instant from);
    
    @Override
    public MessageIterator search(FeedService svc, UUID serverId, Instant from, boolean fromInclusive, Optional<Instant> to, Optional<Boolean> toInclusive,  Optional<Boolean> relay, Predicate<Message>... filters) {
        LOG.entry(getName(), svc, from, fromInclusive, to, toInclusive, serverId, relay, filters);
        MessageIterator result;
        boolean bufferedDataComplete;
        AbstractFeedService service = (AbstractFeedService)svc;
        
        // Fetch any locally buffered data
        if (serverId == null || serverId.equals(service.getServerId())) {
            result = localSearch(svc, from, fromInclusive, to, toInclusive, filters);
            bufferedDataComplete = hasCompleteData(svc,from);
        } else {
            Instant acksFrom = from.minusSeconds(service.getAckTimeout()); // Add extra time to ensure we fetch all the acks
            Predicate<Message>[] adjustedFilters = Arrays.copyOfRange(filters, 0, filters.length+1);
            adjustedFilters[filters.length] = Filters.using(svc).byRemoteTimestamp(serverId, from, to);
            result = localSearch(svc, acksFrom, fromInclusive, to, toInclusive, adjustedFilters);
            bufferedDataComplete = hasCompleteData(svc, acksFrom);
        }

        LOG.debug("Buffered data considered complete: {}", bufferedDataComplete);

        if (!bufferedDataComplete && relay.orElse(true)) {
            try {
                if (result.hasNext()) {
                    Message first = result.next();
                    result = MessageIterator.of(relay(service, serverId, from, fromInclusive, Optional.of(first.getTimestamp()), Optional.of(false), filters), MessageIterator.of(first), result);
                } else {
                    result.close();
                    result = relay(service, serverId, from, fromInclusive, to, toInclusive, filters);
                }
            } catch (FeedExceptions.InvalidPath exp) {
                // Invalid path shouldn't happen here
                throw LOG.throwing(FeedExceptions.runtime(exp));
            }
        }
        
        Stream<MessageIterator> feeds = Stream.concat(
            Stream.of(result), 
            getChildren().map(feed->feed.search(service, serverId, from, fromInclusive, to, toInclusive, relay, filters))
        );
        
        result = MessageIterator.merge(feeds);

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
    
    public MessageIterator relay(FeedService service, UUID serverId, Instant from, boolean fromInclusive, Optional<Instant> to, Optional<Boolean> toInclusive, Predicate<Message>... filters) throws FeedExceptions.InvalidPath {
        LOG.entry(getName(), service, from, fromInclusive, to, toInclusive, serverId, filters);
        MessageIterator result = MessageIterator.EMPTY;
        try (Stream<FeedService> remotes = service.getCluster().getServices(remote -> !Objects.equals(service.getServerId(), remote.getServerId()))) {
            for (FeedService remote : (Iterable<FeedService>)remotes::iterator) {
                MessageIterator.Peekable remoteResult = remote.search(getName(), serverId, from, fromInclusive, to, toInclusive, Optional.of(false), filters).peekable();
                Instant remoteFrom = remoteResult.peek().map(message->message.getTimestamp()).orElse(Instant.MAX);
                LOG.debug("Relay search found messages between {} and {} on serverId {}", from, to, remote.getServerId());
                if (to.isPresent() && (toInclusive.orElse(true) && !remoteFrom.isAfter(to.get()) || remoteFrom.isBefore(to.get()))) {
                    result = MessageIterator.of(remoteResult, result);
                    to = Optional.of(remoteFrom);
                    toInclusive = Optional.of(false);
                }
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
    }
}
