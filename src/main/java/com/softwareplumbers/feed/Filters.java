/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidId;
import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Predicate;

/**
 *
 * @author jonat
 */
public class Filters {
    
    public static final Predicate<Message> NO_ACKS = message->message.getType() != MessageType.ACK;
    public static final Predicate<Message> IS_ACK = message->message.getType() == MessageType.ACK;
    public static final Predicate<Message> POSTED_LOCALLY = message->!message.getRemoteInfo().isPresent();
    
    public static class FromRemote implements Predicate<Message>  {
        public final UUID remote;

        public FromRemote(UUID remote) {
            this.remote = remote;
        }
        
        @Override
        public boolean test(Message message) {
            return message.getRemoteInfo().isPresent() && message.getRemoteInfo().get().serverId.equals(remote);
        }
    }
    
    public static Predicate<Message> fromRemote(UUID remote) { return new FromRemote(remote); }
    
    private FeedService service;
    
    public Filters(FeedService service) { this.service = service; }
    
    public class ByRemoteTimestamp implements Predicate<Message> {
        public final UUID serverId;
        public final Instant from;
        public final Instant to;
        public final Instant initTime;
        
        public ByRemoteTimestamp(UUID serverId, Instant from, Instant to) {
            this.serverId = serverId;
            this.from = from;
            this.to = to;
            this.initTime = service.getCluster().getService(serverId)
                .orElseThrow(()->new RuntimeException("invalid server id " + serverId))
                .getInitTime();
        }
        
        @Override
        public boolean test(Message message) {
            Instant timestamp = message.getTimestamp();
            if (message.getRemoteInfo().isPresent()) {
                // it's a message from a remote
                Message.RemoteInfo remoteInfo = message.getRemoteInfo().get();
                if (remoteInfo.serverId.equals(serverId)) {
                    // the only remote messages we need to futz with are the ones sent by the specified serverId;
                    // in this case we use the remote timestamp already in the message
                    timestamp = remoteInfo.timestamp;
                }
            } else {
                // its a message that was posted locally. In this case we just need to switch the timestamp with
                // the one that was reported in the Ack from the specified server.
                try {
                    timestamp = service.search(message.getName(), Filters.IS_ACK, Filters.fromRemote(serverId))
                       .toStream()
                       .findAny() // Find any ACK from the specified server 
                       .map(Message::getTimestamp)
                       .orElse(timestamp.isBefore(initTime) ? timestamp : initTime); // Haven't received the ack yet, or this is a request from a new node
                } catch (InvalidPath | InvalidId e) {
                    throw FeedExceptions.runtime(e);
                }
            }
            return timestamp.isAfter(from) && !to.isBefore(timestamp);
        };        
    }
    
    public static Filters using(FeedService service) { return new Filters(service); }
    
    public Predicate<Message> byRemoteTimestamp(UUID serverId, Instant from, Instant to) {
        return new ByRemoteTimestamp(serverId, from, to);
    } 
}
