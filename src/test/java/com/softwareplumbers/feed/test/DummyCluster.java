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
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/**
 *
 * @author jonat
 */
public class DummyCluster implements Cluster {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(DummyCluster.class);
    
    private final ArrayList<FeedService> services = new ArrayList<>();
    
    public DummyCluster() {
    }

    @Override
    public Stream<FeedService> getServices(Predicate<FeedService>... filters) {
        Predicate<FeedService> filter = Stream.of(filters).reduce(service->true, Predicate::and);
        return services.stream().filter(filter);
    }
    
    public void register(FeedService service) {
        LOG.entry(service);
        synchronized(this) {
            service.initialize(this);
            services.forEach(existing->existing.monitor(service));
            services.add(service);
        }
        LOG.exit();
    }
}
