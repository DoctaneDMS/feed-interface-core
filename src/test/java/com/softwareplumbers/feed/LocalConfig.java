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
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
    
    @Autowired
    ApplicationContext context;
    
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
    Resolver<Cluster.Host> resolverClusters() {
        return (uri, credentials) -> {
            if (uri.equals(TEST_URI_A)) return Optional.of(context.getBean("testSimpleCluster", Cluster.class).getLocalHost());
            if (uri.equals(TEST_URI_B)) return Optional.of(context.getBean("remoteSimpleCluster", Cluster.class).getLocalHost());
            return Optional.empty();
        };
    }

    @Bean
    @Scope("singleton")
    Cluster testSimpleCluster(
        @Qualifier("resolverClusters") Resolver<Cluster.Host> resolverClusters,
        @Qualifier("testSimpleClusterNodeC") FeedService nodeC
    ) throws IOException {        
        Cluster cluster = new FilesystemCluster(
            Executors.newFixedThreadPool(4), 
            Paths.get(env.getProperty("installation.root")).resolve("cluster.json"), 
            TEST_URI_A,
            resolverClusters
        );
        cluster.register(nodeC);
        return cluster;
    }

    @Bean
    @Scope("singleton")
    Cluster remoteSimpleCluster(
        @Qualifier("resolverClusters") Resolver<Cluster.Host> resolverClusters,
        @Qualifier("testSimpleClusterNodeC") FeedService nodeC
    ) throws IOException {        
        Cluster cluster = new FilesystemCluster(
            Executors.newFixedThreadPool(4), 
            Paths.get(env.getProperty("installation.root")).resolve("cluster.json"), 
            TEST_URI_B,
            resolverClusters
        );
        cluster.register(nodeC);
        return cluster;
    }
    
    @Bean
    @Scope("singleton")
    FeedService testSimpleClusterNodeA(@Qualifier("testSimpleCluster") Cluster cluster) throws URISyntaxException, IOException {
        FeedService nodeA = new DummyFeedService(UUID.randomUUID(), 100000, 2000);
        cluster.getLocalHost().register(nodeA);
        return nodeA;
    }

    @Bean
    @Scope("singleton")
    FeedService testSimpleClusterNodeB(@Qualifier("remoteSimpleCluster") Cluster cluster) throws URISyntaxException, IOException {
        FeedService nodeB = new DummyFeedService(UUID.randomUUID(), 100000, 2000);
        cluster.getLocalHost().register(nodeB);
        return nodeB;
    }  

    @Bean
    @Scope("singleton")
    FeedService testSimpleClusterNodeC() throws URISyntaxException, IOException {
        FeedService nodeC = new DummyFeedService(TEST_UUID_C, 100000, 2000);
        return nodeC;
    }  
}
