/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidId;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;


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
    
    /** Convenience method for receiving messages related to this feed.
     * 
     * Should be the same as calling service.listen(this.getName(), from, serverId)
     * 
     * @param service Service from which to receive messages
     * @param from Timestamp after which we are interested in messages
     * @param serverId Server at which the from timestamp was retrieved
     * @return Future which will complete when messages are received
     */
    default CompletableFuture<MessageIterator> listen(FeedService service, Instant from, UUID serverId) {
        try {
            return service.listen(getName(), from, serverId);
        } catch (FeedExceptions.InvalidPath e) {
            throw new FeedExceptions.BaseRuntimeException(e);
        }
    }
    
    /** Convenience method for receiving messages related to this feed that are posted to this server
     * 
     * Should be the same as calling service.watch(this.getName(), from)
     * 
     * @param service Service from which to receive messages
     * @param from Timestamp after which we are interested in messages
     * @return Future which will complete when messages are received
     */
    default CompletableFuture<MessageIterator> watch(FeedService service, Instant from) {
        try {
            return service.watch(getName(), from);
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
    default Message post(FeedService service, Message message) {
        try {
            return service.post(getName(), message);
        } catch (FeedExceptions.InvalidPath e) {
            throw new FeedExceptions.BaseRuntimeException(e);
        }        
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
            return service.search(getName().addId(id));
        } catch (FeedExceptions.InvalidPath e) {
            throw new FeedExceptions.BaseRuntimeException(e);
        }          
    }
    
    default MessageIterator search(FeedService service, Instant from, UUID serverId, Predicate<Message>... filters) {
        try {
            return service.search(getName(), from, serverId);
        } catch (FeedExceptions.InvalidPath e) {
            throw new FeedExceptions.BaseRuntimeException(e);
        }                  
    }
    
    
    default MessageIterator search(FeedService service, Instant from, boolean fromInclusive, Instant to, boolean toInclusive, UUID serverId, Predicate<Message>... filters) {
        try {
            return service.search(getName(), from, fromInclusive, to, toInclusive, serverId);
        } catch (FeedExceptions.InvalidPath e) {
            throw new FeedExceptions.BaseRuntimeException(e);
        }                  
    }    
}
