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
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;


/** Feed Service interface.
 * 
 * Interface for very simple asynchronous message delivery. Target use case
 * is a RESTful web application server.
 *
 * @author Jonathan Essex.
 */
public interface FeedService {  
    
    /** Listen for all messages on a feed after the given instant.
     * 
     * Listen should return all messages with a timestamp greater than 
     * the given instant. 
     * 
     * @param path Path to feed (must not include a message id)
     * @param after Instant after which we listen for new messages
     * @return Future which will be completed when at least one matching message arrives.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    CompletableFuture<MessageIterator> listen(FeedPath path, Instant after, UUID serverId) throws InvalidPath;
    
    
    /** Watch for messages on a feed posted to this server after the given instant.
     * 
     * Watch is used by remote servers to retrieve messages posted to this server.
     * 
     * @param path Path to feed (must not include a message id)
     * @param from Instant after which we listen for new messages
     * @return Future which will be completed when at least one matching message arrives.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    CompletableFuture<MessageIterator> watch(FeedPath path, Instant from) throws InvalidPath;
    
    /** Get all messages sharing the given message Id.
     * 
     * Typically a message and its related ACKs will share the same message Id. Generally
     * we expect a message to be acknowledged by each server in the cluster, so this method
     * can return up to n+1 messages in a cluster with n servers.
     * 
     * @param messageId The message Id we are searching for
     * @return All messages which share the given id.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidId 
     */
    public MessageIterator search(FeedPath messageId, Predicate<Message>... filters) throws InvalidPath, InvalidId;

    /** Get messages synchronously.
     * 
     * Search will return all messages with a timestamp after the given instant. The serverId
     * flag indicates which server the 'from' timestamp came from (since different servers
     * may  have a different view of time).
     * v
     * @param path Path to feed (must not include a message id)
     * @param from Instant after which we search for new messages
     * @param serverId Server id indicates which server's timestamps to use when comparing with 'from'.
     * @return A MessageIterator WHICH MUST BE CLOSED.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    MessageIterator search(FeedPath path, Instant from, UUID serverId, Predicate<Message>... filters) throws InvalidPath;
    
    /** Get messages synchronously.
     * 
     * Sync will return all messages with a timestamp between the given instants. The serverId
     * flag indicates which server the timestamps came from (since different servers
     * may  have a different view of time).
     * 
     * @param path Path to feed (must not include a message id)
     * @param from Instant from which we search for new messages
     * @param fromInclusive indicates that the returned values should include timestamps equal to from
     * @param to Instant to which we search for new messages
     * @param toInclusive indicates that the returned values should include timestamps equal to from
     * @param serverId Server id indicates which server's timestamps to use when comparing with 'from'.
     * @return A MessageIterator WHICH MUST BE CLOSED.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    MessageIterator search(FeedPath path, Instant from, boolean fromInclusive, Instant to, boolean toInclusive, UUID serverId, Predicate<Message>... filters) throws InvalidPath;
    
    /** Sent a message to a feed.
     * 
     * The service may ignore any timestamp, name, or id specified in the 
     * message.
     * 
     * @param path Path of feed to send message to.
     * @param message
     * @return A message with updated timestamp, name, and id
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    Message post(FeedPath path, Message message) throws InvalidPath;
    
    /** Get the Id of this server instance.
     * 
     * Different servers may have a different view of the sequence in which messages arrive. To
     * avoid potentially dropping messages when a restart or a fail-over occurs, clients need
     * to know which server they are listening to and supply that information when they try
     * to re-connect to a new server. (see the sync method above).
     * 
     * @return 
     */
    UUID getServerId();
    

}
