/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.test;

import com.softwareplumbers.feed.Cluster;
import com.softwareplumbers.feed.FeedService;
import java.util.ArrayList;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 *
 * @author jonat
 */
public class DummyCluster implements Cluster {
    
    private final ArrayList<FeedService> services = new ArrayList<>();
    
    public DummyCluster() {
    }

    @Override
    public Stream<FeedService> getServices(Predicate<FeedService>... filters) {
        Predicate<FeedService> filter = Stream.of(filters).reduce(service->true, Predicate::and);
        return services.stream().filter(filter);
    }
    
    public void init(FeedService service) {
        synchronized(this) {
            services.add(service);
            services.stream().filter(remote->!Objects.equals(remote.getServerId(), service.getServerId())).forEach(remote->service.registerRemote(remote));
        }
    }
}
