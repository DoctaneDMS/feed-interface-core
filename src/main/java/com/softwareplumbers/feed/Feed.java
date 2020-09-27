/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidId;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;


/** Simple feed interface.
 *
 * @author jonathan
 */
public interface Feed {
    
    /** Get the name of the feed.
     * 
     * @return the feed name.
     */
    FeedPath getName();
    
    /** Get the timestamp of the last message on this feed.
     * 
     * May be a cached or local version. For an up-to-date version, use
     * getLastTimestamp(service). This method is used when we are either
     * sure the cached version is up to date, or if we don't care that much
     * about the exact time and want to save a server roundtrip.
     * 
     * @return the timestamp of the lat
     */
    Optional<Instant> getLastTimestamp();
    
    Feed setLastTimestamp(Instant instant);
    
    /** Convenience method for receiving messages related to this feed.
     * 
     * Should be the same as calling service.listen(this.getName(), from, serverId)
     * 
     * @param service Service from which to receive messages
     * @param from Timestamp after which we are interested in messages
     * @param serverId Server at which the from timestamp was retrieved
     * @return Future which will complete when messages are received
     */
    default CompletableFuture<MessageIterator> listen(FeedService service, Instant from, UUID serverId, long timeoutMillis, Predicate<Message>... filters) {
        try {
            return service.listen(getName(), from, serverId, timeoutMillis, filters);
        } catch (FeedExceptions.InvalidPath e) {
            throw new FeedExceptions.BaseRuntimeException(e);
        }
    }
        
    /** Convenience method for posting messages to this feed.
     * 
     * Should be the same as calling service.post(this.getName, message)
     * 
     * @param service Service to which we post a message
     * @param message Sent message
     * @return 
     */
    default Message post(FeedService service, Message message) throws FeedExceptions.InvalidState {
        try {
            return service.post(getName(), message);
        } catch (FeedExceptions.InvalidPath e) {
            throw new FeedExceptions.BaseRuntimeException(e);
        }        
    }
    
    default Message replicate(FeedService service, Message message) throws FeedExceptions.InvalidState {
        return service.replicate(message);
    }

    /** Get all messages sharing the given message Id.
     * 
     * Typically a message and its related ACKs will share the same message Id. Generally
     * we expect a message to be acknowledged by each server in the cluster, so this method
     * can return up to n+1 messages in a cluster with n servers.
     * 
     * @param service Service to search
     * @param id The message Id we are searching for
     * @return All messages which share the given id.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidId 
     */    
    default MessageIterator search(FeedService service, String id, Predicate<Message>... filters) throws InvalidId {
        try {
            return service.search(getName().addId(id), filters);
        } catch (FeedExceptions.InvalidPath e) {
            throw new FeedExceptions.BaseRuntimeException(e);
        }          
    }
    
    default MessageIterator search(FeedService service, UUID serverId, Instant from, Optional<Boolean> relay, Predicate<Message>... filters) {
        return search(service, serverId, from, false, Optional.empty(), Optional.empty(), relay, filters);
    }
    
    default MessageIterator search(FeedService service, UUID serverId, Instant from, Predicate<Message>... filters) {
        return search(service, serverId, from, Optional.of(true), filters);
    }
    
    default MessageIterator search(FeedService service, UUID serverId, Instant from, boolean fromInclusive, Optional<Instant> to, Optional<Boolean> toInclusive, Optional<Boolean> relay, Predicate<Message>... filters) {
        try {
            return service.search(getName(), serverId, from, fromInclusive, to, toInclusive, relay, filters);
        } catch (FeedExceptions.InvalidPath e) {
            throw new FeedExceptions.BaseRuntimeException(e);
        }                  
    }    
    
    default MessageIterator search(FeedService service, UUID serverId, Instant from, boolean fromInclusive, Optional<Instant> to, Optional<Boolean> toInclusive, Predicate<Message>... filters) {
        return search(service, serverId, from, fromInclusive, to, toInclusive, Optional.of(true), filters);
    }
    
    default Optional<Instant> getLastTimestamp(FeedService service) {
        try {
            return service.getLastTimestamp(getName());
        } catch (FeedExceptions.InvalidPath e) {
            throw new FeedExceptions.BaseRuntimeException(e);
        }                  
    }
    
    default JsonObject toJson() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("name", getName().toString());
        getLastTimestamp().ifPresent(ts -> builder.add("lastTimestamp", ts.toString()));
        return builder.build();
    }
    
    public static FeedPath getName(JsonObject object) {
        return FeedPath.valueOf(object.getString("name"));
    }
    
    public static Optional<Instant> getLastTimestamp(JsonObject object) {
        return object.containsKey("lastTimestamp") 
            ? Optional.of(Instant.parse(object.getString("lastTimestamp"))) 
            : Optional.empty();
    }
           
}
