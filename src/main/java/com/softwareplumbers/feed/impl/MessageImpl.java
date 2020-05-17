/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.Message;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonObject;
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
    private long length;
    
    public MessageImpl(String id, FeedPath name, Instant timestamp, JsonObject headers, long length, InputStreamSupplier supplier) {
        this.name = name;
        this.id = id;
        this.timestamp = timestamp;
        this.supplier = supplier;
        this.headers = headers;
        this.length = length;
    }
    
    public MessageImpl(FeedPath name, Instant timestamp, JsonObject headers, InputStream data, long length, boolean temporary) throws IOException {
        this(
            name.part.getId().orElseThrow(()->new RuntimeException("no message id in name")),
            name,
            timestamp,
            headers,
            length,
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
            .add("length", getLength())
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
    
    public MessageImpl setData(InputStreamSupplier data, long length) {
        return new MessageImpl(id, name, timestamp, headers,length, data);
    }
    
    @Override
    public long getLength() {
        if (length >=0) return length;
        if (supplier.isPersistent()) {
            try (InputStream is = supplier.get()) {
                long read = 0;
                long skipped;
                while((skipped = is.skip(Long.MAX_VALUE)) > 0) read += skipped;
                length = read;
                return length;
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        } else {
            throw new UnsupportedOperationException("Can't get length of temporary message");
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
        return new MessageImpl(id, name, timestamp, headers, length, supplier);
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public <T> T writeData(OutputStream os, ErrorHandler<T> errorHandler) throws IOException {
        try (InputStream is = supplier.get()) {
            int read = 0;
            long written = 0L;
            byte[] buffer = new byte[8];
            try {               
                while ((read = is.read(buffer, 0, 8)) >= 0) {
                    os.write(buffer, 0, read);
                    written+=read;
                }
               length = written;
               return null;
            } catch (IOException e) {
                return errorHandler.recover(e, new SequenceInputStream(new ByteArrayInputStream(buffer, 0, read), is));
            }
        }
    }
    
    @Override
    public void writeHeaders(OutputStream os) throws IOException {
        try (JsonWriter writer = Json.createWriter(os)) {
            writer.write(getAllHeaders());
        } catch (JsonException e) {
            throw (IOException)e.getCause();                
        }        
    }
    
    @Override
    public InputStream getHeaderStream() {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
            writeHeaders(byteStream);
            return new ByteArrayInputStream(byteStream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
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
        return "MessageImpl[" + getAllHeaders() + "]";
    }
}
