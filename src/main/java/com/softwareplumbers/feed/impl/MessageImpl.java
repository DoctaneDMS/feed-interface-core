/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.Message;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.time.Instant;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonWriter;
import javax.json.stream.JsonParser;

/**
 *
 * @author jonathan
 */
public class MessageImpl implements Message {
    
    public static final int MAX_HEADER_SIZE = 10000;
    
    private JsonObject headers;
    private FeedPath name;
    private String id;
    private Instant timestamp;
    private InputStreamSupplier supplier;
    
    public MessageImpl(FeedPath name, Instant timestamp, JsonObject headers, InputStream data, boolean temporary) throws IOException {
        this.name = name;
        this.id = name.part.getId().orElseThrow(()->new RuntimeException("no message id in path"));
        this.timestamp = timestamp;
        this.supplier = temporary ? ()->data : InputStreamSupplier.copy(()->data);
        JsonObjectBuilder builder = Json.createObjectBuilder();
        headers.forEach((key, value)->builder.add(key, value));
        builder.add("name", name.toString());
        builder.add("timestamp", timestamp.toString());
        this.headers = builder.build();
    }

    @Override
    public JsonObject header() {
        return headers;
    }

    @Override
    public InputStream getData() throws IOException {
        return supplier.get();
    }

    @Override
    public FeedPath getName() {
        return name;
    }

    @Override
    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public InputStream toStream() throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (JsonWriter writer = Json.createWriter(os)) {
            writer.write(headers);
        }
        try (InputStream is = supplier.get()) {
            int read;
            while ((read = is.read()) >= 0) os.write(read);
        }
        return new ByteArrayInputStream(os.toByteArray());
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
        return "Message[" + getName() + "]";
    }
}
