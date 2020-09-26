/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.FeedService;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import com.softwareplumbers.feed.impl.buffer.MessageBuffer;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Comparator;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/** Generic implementation of a Feed where messages which stores messages in memory.
 *
 * @author Jonathan Essex. 
 */
public class BufferingFeed extends AbstractFeed {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(BufferingFeed.class);
    
    private final MessageBuffer buffer;
    
    public BufferingFeed(MessageBuffer buffer, AbstractFeed parentFeed, String name) {
        super(parentFeed, name);
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
        this.buffer = buffer;
    }
    
    @Override
    public Message[] store(Message... messages) {
        return buffer.addMessages(messages);
    }
           
    private Instant checkpoint() {
        return Stream.concat(Stream.of(
                buffer.checkpoint()), 
                getChildren().map(feed->((BufferingFeed)feed).checkpoint())
            ).min(Comparator.naturalOrder())
            .orElseThrow(()->new RuntimeException("Failed checkpoint"));
    }
    
    @Override
    public MessageIterator search(FeedService service, String id, Predicate<Message>... filter) {
        LOG.entry(getName(), service, id, filter);
        return LOG.exit(buffer.getMessages(id, filter));
    }
    
    @Override
    public MessageIterator localSearch(FeedService svc, Instant from, boolean fromInclusive, Optional<Instant> maybeTo, Optional<Boolean> toInclusive, Predicate<Message>... filters) {
        Instant to = maybeTo.orElseGet(()->getChildren().findAny().isPresent() ? checkpoint() : Instant.MAX);
        return buffer.getMessagesBetween(from, fromInclusive, to, toInclusive.orElse(true), filters);        
    }
    
    @Override
    public boolean hasCompleteData(FeedService svc, Instant from) {
        return buffer.firstTimestamp().map(first->!first.isAfter(from)).orElse(false);
    }
        
    public void dumpState(PrintWriter out) {
        super.dumpState(out);
        buffer.dumpState(out);        
    }
    
    public Optional<Instant> getMyLastTimestamp(FeedService service) {
        return buffer.lastTimestamp();
    }
}
