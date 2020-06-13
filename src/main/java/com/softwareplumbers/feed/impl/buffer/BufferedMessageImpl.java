/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.common.pipedstream.OutputStreamConsumer;
import com.softwareplumbers.feed.FeedExceptions;
import com.softwareplumbers.feed.FeedExceptions.StreamingException;
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
        final String sender;
        final long length;
        
        public Headers(JsonObject allHeaders) {
            this.length = allHeaders.getJsonNumber("length").longValueExact();
            this.headers = allHeaders.getJsonObject("headers");
            this.name = FeedPath.valueOf(allHeaders.getString("name"));
            this.timestamp = Message.getTimestamp(allHeaders).orElse(null);
            this.sender = Message.getSender(allHeaders).orElse(null);
        }
    }
    
    private final InputStreamSupplier data;
    private final InputStreamSupplier headers;  
    private Headers headerCache = null;
    
    public BufferedMessageImpl(InputStreamSupplier headers, InputStreamSupplier data) throws FeedExceptions.StreamingException {
        try {
            this.headers = InputStreamSupplier.copy(headers);
            this.data = InputStreamSupplier.copy(data);
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }
    
    private Headers getAllHeaders() {
        if (headerCache == null) {
            try (JsonParser parser = Json.createParser(headers.get())) {
                parser.next();
                JsonObject json = parser.getObject();
                headerCache = new Headers(json);
            } catch (IOException e) {
                throw FeedExceptions.runtime(e);
            } 
        }
        return headerCache;
    }
    
     
    @Override
    public JsonObject getHeaders() {
        return getAllHeaders().headers;
    }
    
    @Override
    public long getLength() {
        return getAllHeaders().length;
    }

    @Override
    public InputStream getData() {
        try {
            return data.get();
        } catch (IOException e) {
            throw FeedExceptions.runtime(e);
        }
    }
    
    @Override
    public MessageImpl setData(InputStreamSupplier data, long length) {
        return new MessageImpl(getId(), getName(), getSender(), getTimestamp(), getHeaders(),length, data);
    }    

    @Override
    public InputStream getHeaderStream() {
        try {
            return headers.get();
        } catch (IOException e) {
            throw FeedExceptions.runtime(e);
        }
    }

    @Override
    public FeedPath getName() {
        return getAllHeaders().name;
    }

    @Override
    public MessageImpl setName(FeedPath name) {
        return new MessageImpl(name.part.getId().orElseThrow(()->new RuntimeException("Bad name")), name, getSender(), getTimestamp(), getHeaders(), getLength(), data);
    }

    @Override
    public String getSender() {
        return getAllHeaders().sender;
    }

    @Override
    public Message setSender(String sender) {
        return new MessageImpl(getId(), getName(), sender, getTimestamp(), getHeaders(), getLength(), data);
    }
        
    @Override
    public Instant getTimestamp() {
        return getAllHeaders().timestamp;
    }

    @Override
    public Message setTimestamp(Instant timestamp) {
        return new MessageImpl(getId(), getName(), getSender(), timestamp, getHeaders(), getLength(), data);
    }
    
    @Override
    public String getId() {
        return getName().part.getId().get();
    }
    

    
    @Override
    public <T> T writeData(OutputStream out, ErrorHandler<T> errorCallback) throws FeedExceptions.StreamingException {
        try {
            OutputStreamConsumer.of(data).consume(out);
        } catch (IOException e) {            
            return errorCallback.recover(e, null);
        }
        return null;
    }
    
     @Override
    public void writeHeaders(OutputStream out) throws FeedExceptions.StreamingException
    {   
        try {
            OutputStreamConsumer.of(headers).consume(out);
        } catch(IOException e) {
            throw new StreamingException(e);
        }
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
        byte[] truncatedHeaders = new byte[64];
        try (InputStream is = headers.get()) {
            is.read(truncatedHeaders);
            return "BufferedMessage[ " + new String(truncatedHeaders) + "... ]";
        } catch (IOException e) {
            return "BufferedMessage[<unreadable>]";        
        }
    }    
}
