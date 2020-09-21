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
import org.springframework.beans.factory.annotation.Autowired;
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
    
    @Bean
    FeedService testService() {
        return new DummyFeedService(100000, 2000);
    }
    
    @Bean
    @Scope("singleton")
    Cluster testSimpleCluster() throws IOException {        
        return new FilesystemCluster(Paths.get(env.getProperty("installation.root")).resolve("cluster")) {
            @Override
            public FeedService getRemote(URI endpoint) {
                throw new UnsupportedOperationException("Test cluster is local only"); 
            }            
        };
    }
    
    @Bean
    @Scope("singleton")
    FeedService testSimpleClusterNodeA() throws URISyntaxException, IOException {
        FeedService nodeA = new DummyFeedService(100000, 2000);
        testSimpleCluster().register(nodeA, new URI("https://nodeA.dummy.local"));
        return nodeA;
    }

    @Bean
    @Scope("singleton")
    FeedService testSimpleClusterNodeB() throws URISyntaxException, IOException {
        FeedService nodeB = new DummyFeedService(100000, 2000);
        testSimpleCluster().register(nodeB, new URI("https://nodeB.dummy.local"));
        return nodeB;
    }
    
}
