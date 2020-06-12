/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import java.time.Instant;
import java.util.function.Consumer;

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
     * @param callback Callback function which will receive messages
     */
    default void listen(FeedService service, Instant from, Consumer<MessageIterator> callback) {
        try {
            service.listen(getName(), from, callback);
        } catch (FeedExceptions.InvalidPath e) {
            throw new FeedExceptions.BaseRuntimeException(e);
        }
    }
}
