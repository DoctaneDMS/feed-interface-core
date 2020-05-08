/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.Message;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.stream.JsonParser;

/**
 *
 * @author jonathan
 */
public class MessageFactory {
    
    public static final int MAX_HEADER_SIZE = 10000;
    
    public static JsonObject parseHeaders(InputStream data) throws IOException {
        if (!data.markSupported()) throw new RuntimeException("Mark not supported");
        data.mark(MAX_HEADER_SIZE);
        JsonObject result;
        try (JsonParser parser = Json.createParser(data)) {
            parser.next();
            result = parser.getObject();
            data.reset();
            data.skip(parser.getLocation().getStreamOffset());
        }
        return result;
    }
    
    public Message build(FeedPath path, InputStream data) throws IOException {
        if (!data.markSupported()) {
            data = new BufferedInputStream(data);
        }
        return new MessageImpl(path, Instant.now(), parseHeaders(data), data, false);
    }
    
    public Message buildTemporary(FeedPath path, InputStream data) throws IOException {
        if (!data.markSupported()) {
            data = new BufferedInputStream(data);
        }
        return new MessageImpl(path, Instant.now(), parseHeaders(data), data, true);        
    }
    
}
