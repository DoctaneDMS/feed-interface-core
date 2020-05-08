/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.json.JsonObject;

/**
 *
 * @author jonathan
 */
public interface FeedService {    
    void listen(FeedPath path, JsonObject clientData, Consumer<Stream<Message>> messageConsumer, int wait);    
    Message post(FeedPath path, Optional<JsonObject> headers, InputStreamSupplier body);
    Stream<Message> sync(FeedPath path);
}
