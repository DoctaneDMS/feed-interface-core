/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidId;
import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

/**
 *
 * @author jonat
 */
public class Filters {
    
    public static class RemotablePredicate implements Predicate<Message> {
        
        public enum Type {
            NO_ACKS,
            IS_ACK,
            POSTED_LOCALLY,
            FROM_REMOTE,
            BY_REMOTE_TIMESTAMP
        }
        
        private Predicate<Message> localPredicate;
        private Type type;

        @Override
        public boolean test(Message message) {
            return localPredicate.test(message);
        }
        
        public RemotablePredicate(Type type, Predicate<Message> localPredicate) {
            this.type = type;
            this.localPredicate = localPredicate;
        }
        
        public JsonObjectBuilder addParams(JsonObjectBuilder builder) {
            return builder;
        }
        
        public JsonObject toJson() {
            return addParams(Json.createObjectBuilder().add("type", type.toString())).build();
        }     
        
        @Override
        public String toString() {
            return toJson().toString();
        }
    }
    
    public static final Predicate<Message> NO_ACKS = new RemotablePredicate(RemotablePredicate.Type.NO_ACKS, message->message.getType() != MessageType.ACK);
    public static final Predicate<Message> IS_ACK = new RemotablePredicate(RemotablePredicate.Type.IS_ACK, message->message.getType() == MessageType.ACK);
    public static final Predicate<Message> POSTED_LOCALLY = new RemotablePredicate(RemotablePredicate.Type.POSTED_LOCALLY, message->!message.getRemoteInfo().isPresent());
    
    public static class FromRemote extends RemotablePredicate  {
        public final UUID remote;

        public FromRemote(UUID remote) {
            super(Type.FROM_REMOTE, message->message.getRemoteInfo().isPresent() && message.getRemoteInfo().get().serverId.equals(remote));
            this.remote = remote;
        }
        
        @Override
        public JsonObjectBuilder addParams(JsonObjectBuilder builder) {
            return builder.add("remote", remote.toString());
        }
        
        @Override
        public boolean equals(Object other) {
            return other instanceof FromRemote && Objects.equals(remote, ((FromRemote)other).remote);
        }
        
        @Override
        public int hashCode() {
            return remote.hashCode();
        }
    }
    
    public static Predicate<Message> fromRemote(UUID remote) { return new FromRemote(remote); }
        
    private final FeedService service;
    
    public Filters(FeedService service) { this.service = service; }

    public static boolean testRemoteTimestamp(FeedService service, Message message, UUID serverId, Instant from, Optional<Instant> to, Instant initTime) {
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
        return timestamp.isAfter(from) && (!to.isPresent() || !to.get().isBefore(timestamp));
    };      
    
    public static Instant getInitTime(FeedService service, UUID serverId) {
        return service.getCluster()
                .orElseThrow(()->new RuntimeException("service must be registered to a cluster to search by a remote timestamp"))    
                .getService(serverId)
                .orElseThrow(()->new RuntimeException("invalid server id " + serverId))
                .getInitTime();
    }
    
    public static class ByRemoteTimestamp extends RemotablePredicate {
        public final UUID serverId;
        public final Instant from;
        public final Optional<Instant> to;
        
        public ByRemoteTimestamp(FeedService service, UUID serverId, Instant from, Optional<Instant> to) {
            super(Type.BY_REMOTE_TIMESTAMP, message->testRemoteTimestamp(service, message, serverId, from, to, getInitTime(service, serverId)));
            this.serverId = serverId;
            this.from = from;
            this.to = to;
        }
        
        public JsonObjectBuilder addParams(JsonObjectBuilder builder) {
            builder = builder.add("serviceId", serverId.toString()).add("from", from.toString());
            if (to.isPresent()) builder = builder.add("to", to.get().toString());
            return builder;
        }   
        
        @Override
        public boolean equals(Object other) {
            return other instanceof ByRemoteTimestamp 
                && Objects.equals(serverId, ((ByRemoteTimestamp)other).serverId)
                && Objects.equals(from, ((ByRemoteTimestamp)other).from)
                && Objects.equals(to, ((ByRemoteTimestamp)other).to);
        }
        
        @Override
        public int hashCode() {
            return from.hashCode();
        }        
    }
    
    public Optional<Predicate<Message>> fromJson(JsonObject object) {
        RemotablePredicate.Type type = RemotablePredicate.Type.valueOf(object.getString("type"));
        switch(type) {
            case NO_ACKS: return Optional.of(NO_ACKS);
            case IS_ACK: return Optional.of(IS_ACK);
            case POSTED_LOCALLY: return Optional.of(POSTED_LOCALLY);
            case FROM_REMOTE: return Optional.of(new FromRemote(UUID.fromString(object.getString("remote"))));
            case BY_REMOTE_TIMESTAMP: 
                return Optional.of(new ByRemoteTimestamp(
                    service,
                    UUID.fromString(object.getString("serviceId")),
                    Instant.parse(object.getString("from")),
                    object.containsKey("to") ? Optional.of(Instant.parse(object.getString("to"))) : Optional.empty()
                ));
            default: return Optional.empty();
        }        
    }
    
    public Predicate<Message>[] fromJson(JsonArray predicates) {
        Predicate<Message>[] result = new Predicate[predicates.size()];
        for (int i = 0; i < result.length; i++) 
            result[i] = fromJson(predicates.getJsonObject(i)).orElseThrow(()->new RuntimeException("can't understand predicate"));
        return result;
    }
    
    public static Filters using(FeedService service) { return new Filters(service); }
    
    public Predicate<Message> byRemoteTimestamp(UUID serverId, Instant from, Optional<Instant> to) {
        return new ByRemoteTimestamp(service, serverId, from, to);
    } 
    
    public static JsonArray toJson(Predicate<Message>[] predicates) {
        JsonArrayBuilder builder = Json.createArrayBuilder();
        Stream.of(predicates)
            .filter(RemotablePredicate.class::isInstance)
            .map(RemotablePredicate.class::cast)
            .forEach(predicate->builder.add(predicate.toJson()));
        return builder.build();
    }
    
    public static Predicate<Message>[] local(Predicate<Message>[] predicates) {
        return Stream.of(predicates)
            .filter(predicate->!(predicate instanceof RemotablePredicate))
            .toArray(Predicate[]::new);
    }
}
