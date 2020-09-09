/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 *
 * @author jonat
 */
public interface Cluster {
    
    default Optional<FeedService> getService(UUID id) {
        return getServices(service->Objects.equals(service.getServerId(), id)).findAny();
    }
    
    Stream<FeedService> getServices(Predicate<FeedService>... filters);
    
    void init(FeedService service);
}
