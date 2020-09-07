/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import java.util.UUID;
import java.util.stream.Stream;

/**
 *
 * @author jonat
 */
public interface Cluster {
    FeedService getService(UUID id);
    Stream<FeedService> getServices();
}
