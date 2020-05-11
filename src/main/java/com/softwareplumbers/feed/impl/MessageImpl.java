/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.Message;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriter;

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
    
    public MessageImpl(String id, FeedPath name, Instant timestamp, JsonObject headers, InputStreamSupplier supplier) {
        this.name = name;
        this.id = id;
        this.timestamp = timestamp;
        this.supplier = supplier;
        this.headers = headers;
    }
    
    public MessageImpl(FeedPath name, Instant timestamp, JsonObject headers, InputStream data, boolean temporary) throws IOException {
        this(
            name.part.getId().orElseThrow(()->new RuntimeException("no message id in name")),
            name,
            timestamp,
            headers,
            temporary ? ()->data : InputStreamSupplier.copy(()->data)
        );
    }
    
    @Override
    public JsonObject getHeaders() {
        return headers;
    }
    
    private JsonObject getAllHeaders() {
        return Json.createObjectBuilder()
            .add("name", name.toString())
            .add("timestamp", timestamp.toString())
            .add("headers", headers)
            .build();
    }

    @Override
    public InputStream getData() {
        try {
            return supplier.get();
        }  catch (IOException e) {
            throw new RuntimeException(e);
        }
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
    public Message setTimestamp(Instant timestamp) {
        return new MessageImpl(id, name, timestamp, headers, supplier);
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public <T> T write(OutputStream os, ErrorHandler<T> errorHandler) throws IOException {
        try (JsonWriter writer = Json.createWriter(os)) {
            writer.write(getAllHeaders());
        } catch (JsonException e) {
            return errorHandler.recover((IOException)e.getCause(), null);                
        }
    
        try (InputStream is = supplier.get()) {
            int read;
            try {
                while ((read = is.read()) >= 0) os.write(read);
                return null;
            } catch (IOException e) {
                return errorHandler.recover(e, is);
            }
        }
    }
    
    @Override
    public InputStream toStream() throws IOException {
        return InputStreamSupplier.pipe(output->write(output));
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
        return "MessageImpl[" + getAllHeaders() + "]";
    }
}
