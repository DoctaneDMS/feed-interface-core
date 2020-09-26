/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

/**
 *
 * @author jonat
 */
public interface FeedServiceManager {
    Cluster getCluster();
    void register(FeedService service);
    void deregister(FeedService service);
}
