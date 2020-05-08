/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Objects;
import javax.json.JsonObject;

/**
 *
 * @author jonathan
 */
public interface Message {
    
    public JsonObject header();
    public InputStream getData() throws IOException;
    public FeedPath getName();
    public Instant getTimestamp();
    public default String getId() {
        return getName().part.getId().orElseThrow(()->new RuntimeException("Invalid message id"));
    }
    public InputStream toStream() throws IOException;   
    public static boolean equals(Message a, Message b) {
        return Objects.equals(a.header(), b.header());
    }
}
