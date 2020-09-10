/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.test.DummyCluster;
import com.softwareplumbers.feed.test.DummyFeedService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 *
 * @author jonathan
 */
@Configuration
public class LocalConfig {
    
    @Bean
    FeedService testService() {
        return new DummyFeedService(100000, 2000);
    }
    
    @Bean
    @Scope("singleton")
    Cluster testSimpleCluster() {
        return new DummyCluster();
    }
    
    @Bean
    @Scope("singleton")
    FeedService testSimpleClusterNodeA() {
        FeedService nodeA = new DummyFeedService(100000, 2000);
        testSimpleCluster().register(nodeA);
        return nodeA;
    }

    @Bean
    @Scope("singleton")
    FeedService testSimpleClusterNodeB() {
        FeedService nodeB = new DummyFeedService(100000, 2000);
        testSimpleCluster().register(nodeB);
        return nodeB;
    }
    
}
