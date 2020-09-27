/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.Feed;
import com.softwareplumbers.feed.FeedPath;
import java.time.Instant;
import java.util.Optional;
import javax.json.JsonObject;

/**
 *
 * @author jonathan
 */
public class FeedImpl implements Feed {
    
    private final FeedPath name;
    private final Optional<Instant> lastTimestamp;
    
    public FeedImpl(FeedPath name, Optional<Instant> lastTimestamp) {
        this.name = name;
        this.lastTimestamp = lastTimestamp;
    }
    
    @Override
    public Feed setLastTimestamp(Optional<Instant> lastTimestamp) {
        return new FeedImpl(name, lastTimestamp);
    }

    @Override
    public FeedPath getName() {
        return name;
    }
    
    @Override
    public Optional<Instant> getLastTimestamp() {
        return this.lastTimestamp;
    }
    
    @Override
    public String toString() {
        return "FeedImpl[" + name + "]";
    }
    
    public static FeedImpl fromJson(JsonObject obj) {
        return new FeedImpl(Feed.getName(obj), Feed.getLastTimestamp(obj));
    }
}
