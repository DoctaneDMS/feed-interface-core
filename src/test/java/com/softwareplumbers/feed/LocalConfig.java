/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.impl.FilesystemCluster;
import com.softwareplumbers.feed.impl.Resolver;
import com.softwareplumbers.feed.test.DummyFeedService;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;

/**
 *
 * @author jonathan
 */
@Configuration
public class LocalConfig {

    @Autowired
    Environment env;    
    
    @Bean
    FeedService testService() {
        return new DummyFeedService(UUID.randomUUID(), 100000, 2000);
    }
    
    private static final URI TEST_URI_A = URI.create("http://testA.net");
    private static final URI TEST_URI_B = URI.create("http://testB.net");
    private static final URI TEST_URI_C = URI.create("http://testC.net");
    public static final UUID TEST_UUID_C = UUID.fromString("da7aa4a9-cb2d-4525-aa87-184f1ae1f642");
    
    @Bean
    @Scope("singleton") 
    Resolver<FeedService> resolverFeeds(@Qualifier("testSimpleClusterNodeA") FeedService a, @Qualifier("testSimpleClusterNodeB") FeedService b, @Qualifier("testSimpleClusterNodeC") FeedService c) {
        return (uri, credentials) -> {
            if (uri.equals(TEST_URI_A)) return Optional.of(a);
            if (uri.equals(TEST_URI_B)) return Optional.of(b);
            if (uri.equals(TEST_URI_C)) return Optional.of(c);
            return Optional.empty();
        };
    }

    @Bean
    @Scope("singleton") 
    Resolver<Cluster> resolverClusters(@Lazy @Qualifier("testSimpleCluster") Cluster a, @Lazy @Qualifier("remoteSimpleCluster") Cluster b) {
        return (uri, credentials) -> {
            if (uri.equals(TEST_URI_A)) return Optional.of(a);
            if (uri.equals(TEST_URI_B)) return Optional.of(b);
            return Optional.empty();
        };
    }

    @Bean
    @Scope("singleton")
    Cluster testSimpleCluster(
        @Qualifier("resolverFeeds") Resolver<FeedService> resolverFeeds, 
        @Qualifier("resolverClusters") Resolver<Cluster> resolverClusters,
        @Qualifier("testSimpleClusterNodeA") FeedService nodeA
    ) throws IOException {        
        FilesystemCluster cluster = new FilesystemCluster(
            Executors.newFixedThreadPool(4), 
            Paths.get(env.getProperty("installation.root")).resolve("cluster"), 
            resolverFeeds, 
            resolverClusters
        );
        cluster.clean();
        cluster.register(nodeA, TEST_URI_A);
        cluster.register(TEST_UUID_C, TEST_URI_C);
        return cluster;
    }

    @Bean
    @Scope("singleton")
    Cluster remoteSimpleCluster(
        @Qualifier("resolverFeeds") Resolver<FeedService> resolverFeeds, 
        @Qualifier("resolverClusters") Resolver<Cluster> resolverClusters,
        @Qualifier("testSimpleClusterNodeB") FeedService nodeB
    ) throws IOException {        
        Cluster cluster = new FilesystemCluster(
            Executors.newFixedThreadPool(4), 
            Paths.get(env.getProperty("installation.root")).resolve("cluster"), 
            resolverFeeds, 
            resolverClusters
        );
        cluster.register(nodeB, TEST_URI_B);
        //cluster.register(TEST_UUID_C, TEST_URI_C);
        return cluster;
    }
    
    @Bean
    @Scope("singleton")
    FeedService testSimpleClusterNodeA() throws URISyntaxException, IOException {
        FeedService nodeA = new DummyFeedService(UUID.randomUUID(), 100000, 2000);
        return nodeA;
    }

    @Bean
    @Scope("singleton")
    FeedService testSimpleClusterNodeB() throws URISyntaxException, IOException {
        FeedService nodeB = new DummyFeedService(UUID.randomUUID(), 100000, 2000);
        return nodeB;
    }  

    @Bean
    @Scope("singleton")
    FeedService testSimpleClusterNodeC() throws URISyntaxException, IOException {
        FeedService nodeC = new DummyFeedService(TEST_UUID_C, 100000, 2000);
        return nodeC;
    }  
}
