/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.Feed;
import com.softwareplumbers.feed.FeedPath;

/**
 *
 * @author jonathan
 */
public class FeedImpl implements Feed {
    
    private final FeedPath name;
    
    public FeedImpl(FeedPath name) {
        this.name = name;
    }

    @Override
    public FeedPath getName() {
        return name;
    }
    
    @Override
    public String toString() {
        return "FeedImpl[" + name + "]";
    }
}
