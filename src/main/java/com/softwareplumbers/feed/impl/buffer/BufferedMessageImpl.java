/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.common.pipedstream.OutputStreamConsumer;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.impl.MessageImpl;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.stream.JsonParser;

/**
 *
 * @author jonathan
 */
public class BufferedMessageImpl implements Message {
    
    private static class Headers {
        final JsonObject headers;
        final FeedPath name;
        final Instant timestamp;
        final long headerSize;
        
        public Headers(JsonObject allHeaders, long headerSize) {
            this.headerSize = headerSize;
            this.headers = allHeaders.getJsonObject("headers");
            this.name = FeedPath.valueOf(allHeaders.getString("name"));
            this.timestamp = Instant.parse(allHeaders.getString("timestamp"));
        }
    }
    
    private final InputStreamSupplier data;
    private Headers allHeaders = null;
    
    public BufferedMessageImpl(InputStreamSupplier data) throws IOException {
        this.data = InputStreamSupplier.copy(data);
    }
    
    private Headers getAllHeaders() {
        if (allHeaders == null) {
            try (JsonParser parser = Json.createParser(data.get())) {
                parser.next();
                JsonObject json = parser.getObject();
                allHeaders = new Headers(json, parser.getLocation().getStreamOffset());
            } catch (IOException e) {
                throw new RuntimeException(e);
            } 
        }
        return allHeaders;
    }
    
     
    @Override
    public JsonObject getHeaders() {
        return getAllHeaders().headers;
    }

    @Override
    public InputStream getData() {
        try {
            InputStream is = data.get();
            is.skip(getAllHeaders().headerSize);
            return is;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FeedPath getName() {
        return getAllHeaders().name;
    }

    @Override
    public Instant getTimestamp() {
        return getAllHeaders().timestamp;
    }

    @Override
    public Message setTimestamp(Instant timestamp) {
        return new MessageImpl(getId(), getName(), timestamp, getHeaders(), ()->getData());
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
    public <T> T write(OutputStream out, ErrorHandler<T> errorCallback) throws IOException {
        try {
            OutputStreamConsumer.of(data).consume(out);
        } catch (IOException e) {            
            return errorCallback.recover(e, null);
        }
        return null;
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
        byte[] truncatedHeaders = new byte[128];
        try (InputStream is = data.get()) {
            is.read(truncatedHeaders);
            return "BufferedMessage[ " + new String(truncatedHeaders) + "... ]";
        } catch (IOException e) {
            return "BufferedMessage[<unreadable>]";        
        }
    }    
}
