/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.Message;
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
public class BufferedMessageImpl implements Message {
    
    private static class Headers {
        final JsonObject allHeaders;
        final FeedPath name;
        final Instant timestamp;
        final long headerSize;
        
        public Headers(JsonObject allHeaders, long headerSize) {
            this.headerSize = headerSize;
            this.allHeaders = allHeaders;
            this.name = FeedPath.valueOf(allHeaders.getString("name"));
            this.timestamp = Instant.parse(allHeaders.getString("timestamp"));
        }
    }
    
    private final InputStreamSupplier data;
    private Headers headers = null;
    
    public BufferedMessageImpl(InputStreamSupplier data) throws IOException {
        this.data = InputStreamSupplier.copy(data);
    }
    
    private Headers getHeaders() {
        if (headers == null) {
            try (JsonParser parser = Json.createParser(data.get())) {
                parser.next();
                JsonObject allHeaders = parser.getObject();
                headers = new Headers(allHeaders, parser.getLocation().getStreamOffset());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return headers;
    }
    
     
    @Override
    public JsonObject header() {
        return getHeaders().allHeaders;
    }

    @Override
    public InputStream getData() throws IOException {
        InputStream is = data.get();
        is.skip(getHeaders().headerSize);
        return is;
    }

    @Override
    public FeedPath getName() {
        return getHeaders().name;
    }

    @Override
    public Instant getTimestamp() {
        return getHeaders().timestamp;
    }

    @Override
    public String getId() {
        return getName().part.getId().get();
    }
    
    @Override
    public InputStream toStream() throws IOException {
        return data.get();
    }
    
    @Override
    public boolean equals(Object other) {
        return other instanceof Message && Message.equals(this, (Message)other);
    }
    @Override
    public int hashCode() {
        return getName().hashCode();
    }
    
    @Override
    public String toString() {
        return "BufferedMessage[" + getName() + "]";
    }    
}
