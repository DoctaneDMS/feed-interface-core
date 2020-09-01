/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import java.time.Instant;
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
    
    /** Get the id of the feed.
     * 
     * @return the feed id.
     */
    String getId();
    
    /** Convenience method for receiving messages related to this feed.
     * 
     * Should be the same as calling service.listen(this.getName(), from, callback)
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
}
