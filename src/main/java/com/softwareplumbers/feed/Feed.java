/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidId;
import java.io.IOException;
import java.io.Writer;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;


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
     * Should be the same as calling service.listen(this.getName(), from)
     * 
     * @param service Service from which to receive messages
     * @param from Timestamp after which we are interested in messages
     * @return Future which will complete when messages are received
     */
    default CompletableFuture<MessageIterator> listen(FeedService service, Instant from) {
        try {
            return service.listen(getName(), from);
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
    
    default MessageIterator getMessages(FeedService service, String id) throws InvalidId {
        try {
            return service.getMessages(getName().addId(id));
        } catch (FeedExceptions.InvalidPath e) {
            throw new FeedExceptions.BaseRuntimeException(e);
        }          
    }
    
}
