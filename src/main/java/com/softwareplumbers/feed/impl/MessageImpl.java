/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.feed.FeedExceptions;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonWriter;

/**
 *
 * @author jonathan
 */
public class MessageImpl implements Message {
    
    public static final int MAX_HEADER_SIZE = 10000;
    private static final InputStream NULL_STREAM = new InputStream() { public int read() { return -1; } };
    
    private final JsonObject headers;
    private final FeedPath name;
    private final String id;
    private final Instant timestamp;
    private final Optional<UUID> serverId;
    private final InputStreamSupplier supplier;
    private final String sender;
    private long length;
    private final MessageType type;
    private final Optional<RemoteInfo> remoteInfo;
    
    public MessageImpl(MessageType type, String id, FeedPath name, String sender, Instant timestamp, Optional<UUID> serverId, Optional<RemoteInfo> remoteInfo, JsonObject headers, long length, InputStreamSupplier supplier) {
        this.name = name;
        this.id = id;
        this.timestamp = timestamp;
        this.serverId = serverId;
        this.supplier = supplier;
        this.headers = headers;
        this.length = length;
        this.sender = sender;
        this.type = type;
        this.remoteInfo = remoteInfo;
    }
    
    private static InputStreamSupplier supplierOf(InputStream data, boolean temporary) {
        try {
            return temporary ? ()->data : InputStreamSupplier.copy(()->data);
        } catch (IOException e) {
            throw FeedExceptions.runtime(e);
        }
    }
    
    public MessageImpl(MessageType type, FeedPath name, String sender, Instant timestamp, Optional<UUID> serverId, Optional<RemoteInfo> remoteInfo, JsonObject headers, InputStream data, long length, boolean temporary) {
        this(
            type,
            name == null || name.isEmpty() ? null : name.part.getId().orElse(null),
            name,
            sender,
            timestamp,
            serverId,
            remoteInfo,
            headers,
            length,
            supplierOf(data, temporary)
        );
    }
    
    public MessageImpl(MessageType type, FeedPath name, String sender, Instant timestamp, Optional<UUID> serverId, Optional<RemoteInfo> remoteInfo, JsonObject headers) {
        this.name = name;
        this.id = name == null || name.isEmpty() ? null : name.part.getId().orElse(null);
        this.timestamp = timestamp;
        this.serverId = serverId;
        this.supplier = ()->NULL_STREAM;
        this.headers = headers;
        this.length = 0;
        this.sender = sender;
        this.type = type;        
        this.remoteInfo = remoteInfo;
    }
    
    @Override
    public JsonObject getHeaders() {
        return headers;
    }
    
    private JsonObject getAllHeaders() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        if (name != null) builder = builder.add("name", name.toString());
        if (timestamp != null) builder = builder.add("timestamp", timestamp.toString());
        if (sender != null) builder = builder.add("sender", sender);
        if (type != MessageType.NONE) builder.add("type", type.toString());
        if (serverId.isPresent()) builder.add("serverId", serverId.get().toString());
        if (remoteInfo.isPresent()) builder.add("remoteInfo", remoteInfo.get().toJson());

        return builder
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
        return new MessageImpl(type, id, name, sender, timestamp, serverId, remoteInfo, headers,length, data);
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
    public MessageImpl setName(FeedPath name) {
        return new MessageImpl(type, name.part.getId().orElseThrow(()->new RuntimeException("Bad name")), name, sender, timestamp, serverId, remoteInfo, headers, length, supplier);
    }

    @Override
    public Instant getTimestamp() {
        return timestamp;
    }
    
    @Override
    public Message setTimestamp(Instant timestamp) {
        return new MessageImpl(type, id, name, sender, timestamp, serverId, remoteInfo, headers, length, supplier);
    }
    
    @Override
    public Optional<UUID> getServerId() {
        return serverId;
    }
    
    @Override
    public Message setServerId(UUID serverId) {
        return new MessageImpl(type, id, name, sender, timestamp, Optional.of(serverId), remoteInfo, headers, length, supplier);        
    }
    
    @Override
    public String getSender() {
        return sender;
    }
    
    @Override
    public Message setSender(String sender) {
        return new MessageImpl(type, id, name, sender, timestamp, serverId, remoteInfo, headers, length, supplier);
    }
    
    @Override
    public MessageType getType() {
        return type;
    }
    
    @Override
    public Message setType(MessageType type) {
        return new MessageImpl(type, id, name, sender, timestamp, serverId, remoteInfo, headers, length, supplier);        
    }
    
    @Override
    public Optional<RemoteInfo> getRemoteInfo() {
        return remoteInfo;
    }
    
    @Override
    public Message setRemoteInfo(RemoteInfo remoteInfo) {
        return new MessageImpl(type, id, name, sender, timestamp, serverId, Optional.of(remoteInfo), headers, length, supplier);                
    }
    
    @Override
    public Message localizeTimestamp(UUID serverId, Instant timestamp) {
        return new MessageImpl(type, id, name, sender, timestamp, Optional.of(serverId), Optional.of(new RemoteInfo(this.serverId.orElse(null), this.timestamp)), headers, length, supplier);
    }
    
    @Override
    public String getId() {
        return id;
    }

    @Override
    public <T> T writeData(OutputStream os, ErrorHandler<T> errorHandler) throws FeedExceptions.StreamingException {
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
        } catch (IOException e) {
            throw FeedExceptions.runtime(e);
        }
    }
    
    @Override
    public void writeHeaders(OutputStream os) throws FeedExceptions.StreamingException {
        try (JsonWriter writer = Json.createWriter(os)) {
            writer.write(getAllHeaders());
        } catch (JsonException e) {
            throw new FeedExceptions.StreamingException((IOException)e.getCause());                
        }        
    }
    
    @Override
    public InputStream getHeaderStream() {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
            writeHeaders(byteStream);
            return new ByteArrayInputStream(byteStream.toByteArray());
        } catch (FeedExceptions.StreamingException e) {
            throw FeedExceptions.runtime(e);
        } catch (IOException e) {
            throw FeedExceptions.runtime(e);
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
        if (supplier.isPersistent())
            return "MessageImpl[" + getAllHeaders() + "]";
        else
            return "MessageImpl[" + getHeaders() + "]";
    }
    
    public static Message acknowledgement(Message message, Instant atTime, UUID atServer) {
        return new MessageImpl(MessageType.ACK, message.getName(), null, atTime, Optional.of(atServer), Optional.empty(), JsonObject.EMPTY_JSON_OBJECT);
    }
}
