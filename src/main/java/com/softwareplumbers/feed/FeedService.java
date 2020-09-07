/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidId;
import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import java.io.IOException;
import java.io.Writer;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;


/** Feed Service interface.
 * 
 * Interface for very simple asynchronous message delivery. Target use case
 * is a RESTful web application server.
 *
 * @author Jonathan Essex.
 */
public interface FeedService {  
    
    /** Listen for messages on a feed after the given instant.
     * 
     * Listen should never return a message with a timestamp less than or
     * equal to the given instant. However, it may not return all messages which
     * have a greater timestamp - a limited buffer should be assumed, so setting
     * Instant to a time significantly before the last message received is
     * discouraged.
     * 
     * @param path Path to feed (must not include a message id)
     * @param after Instant after which we listen for new messages
     * @return Future which will be completed when at least one matching message arrives.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    CompletableFuture<MessageIterator> listen(FeedPath path, Instant after) throws InvalidPath;
    
    /** Get messages synchronously.
     * 
     * Sync will return all messages with a timestamp after the given instant. When starting up a client,
     * or reconnecting after a given server goes down, we use 'sync' to resynchronize the client's view
     * of events with the view of events as seen by the new server.
     * 
     * @param path Path to feed (must not include a message id)
     * @param from Instant after which we listen for new messages
     * @param serverId Server id indicates which server's timestamps to use when comparing with 'from'.
     * @return A MessageIterator WHICH MUST BE CLOSED.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    MessageIterator sync(FeedPath path, Instant from, UUID serverId) throws InvalidPath;
    
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
    
    public MessageIterator getMessages(FeedPath addId) throws InvalidPath, InvalidId;
}
