/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.impl.FilesystemCluster;
import com.softwareplumbers.feed.test.DummyFeedService;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
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
public class LocalConfigWithResolver {

    @Autowired
    Environment env;    
    
    private static final URI TEST_URI_A = URI.create("http://testA.net");
    private static final URI TEST_URI_B = URI.create("http://testB.net");
    
    @Bean
    @Scope("singleton") 
    Map<URI, FeedService> resolverFeeds(@Qualifier("testSimpleClusterNodeA") FeedService a, @Qualifier("testSimpleClusterNodeB") FeedService b) {
        return new HashMap<URI, FeedService>() {{
            put(TEST_URI_A, a);
            put(TEST_URI_B, b);
        }};
    }

    @Bean
    @Scope("singleton") 
    Map<URI, Cluster> resolverClusters(@Lazy @Qualifier("testSimpleCluster") Cluster a, @Lazy @Qualifier("remoteSimpleCluster") Cluster b) {
        return new HashMap<URI, Cluster>() {{
            put(TEST_URI_A, a);
            put(TEST_URI_B, b);
        }};
    }

    @Bean
    @Scope("singleton")
    Cluster testSimpleCluster(
        @Qualifier("resolverFeeds") Map<URI, FeedService> resolverFeeds, 
        @Qualifier("resolverClusters") Map<URI, Cluster> resolverClusters,
        @Qualifier("testSimpleClusterNodeA") FeedService nodeA
    ) throws IOException {        
        Cluster cluster = new FilesystemCluster(
            Executors.newFixedThreadPool(4), 
            Paths.get(env.getProperty("installation.root")).resolve("cluster"), 
            resolverFeeds::get, 
            resolverClusters::get
        );
        cluster.register(nodeA, TEST_URI_A);
        return cluster;
    }

    @Bean
    @Scope("singleton")
    Cluster remoteSimpleCluster(
        @Qualifier("resolverFeeds") Map<URI, FeedService> resolverFeeds, 
        @Qualifier("resolverClusters") Map<URI, Cluster> resolverClusters,
        @Qualifier("testSimpleClusterNodeB") FeedService nodeB
    ) throws IOException {        
        Cluster cluster = new FilesystemCluster(
            Executors.newFixedThreadPool(4), 
            Paths.get(env.getProperty("installation.root")).resolve("cluster"), 
            resolverFeeds::get, 
            resolverClusters::get
        );
        cluster.register(nodeB, TEST_URI_B);
        return cluster;
    }
    
    @Bean
    @Scope("singleton")
    FeedService testSimpleClusterNodeA() throws URISyntaxException, IOException {
        FeedService nodeA = new DummyFeedService(100000, 2000);
        return nodeA;
    }

    @Bean
    @Scope("singleton")
    FeedService testSimpleClusterNodeB() throws URISyntaxException, IOException {
        FeedService nodeB = new DummyFeedService(100000, 2000);
        return nodeB;
    }
    
}
