/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.test.DummyFeedService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
    
}
