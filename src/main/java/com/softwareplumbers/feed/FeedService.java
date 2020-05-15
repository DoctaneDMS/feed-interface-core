/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import java.time.Instant;
import java.util.function.Consumer;

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
     * @param messageConsumer Callback which will be invoked when at least one matching message arrives.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    void listen(FeedPath path, Instant after, Consumer<MessageIterator> messageConsumer) throws InvalidPath;
    
    /** Get messages synchronously.
     * 
     * Sync will return all messages with a timestamp after the given instant.
     * 
     * @param path Path to feed (must not include a message id)
     * @param from Instant after which we listen for new messages
     * @return A MessageIterator WHICH MUST BE CLOSED.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    MessageIterator sync(FeedPath path, Instant from) throws InvalidPath;
    
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
    void dumpState();
}
