/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import java.time.Instant;
import java.util.function.Consumer;

/**
 *
 * @author jonathan
 */
public interface FeedService {    
    void listen(FeedPath path, Instant after, Consumer<MessageIterator> messageConsumer);
    MessageIterator sync(FeedPath path);
    Message post(Message message);
}
