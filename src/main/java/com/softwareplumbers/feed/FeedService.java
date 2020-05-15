/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import java.time.Instant;
import java.util.function.Consumer;

/**
 *
 * @author jonathan
 */
public interface FeedService {    
    void listen(FeedPath path, Instant after, Consumer<MessageIterator> messageConsumer) throws InvalidPath;
    MessageIterator sync(FeedPath path, Instant from) throws InvalidPath;
    Message post(FeedPath path, Message message) throws InvalidPath;
    void dumpState();
}
