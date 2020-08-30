/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.FeedExceptions;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.MessageIterator;
import java.io.BufferedInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.stream.JsonParser;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/** Creates messages.
 *
 * @author Jonathan Essex
 */
public class MessageFactory {
    
    public static final XLogger LOG = XLoggerFactory.getXLogger(MessageFactory.class);
    
    private static class SubStream extends FilterInputStream {
        public SubStream(InputStream is) { super(is); }
        @Override
        public void close() { /* Don't close the stream  */ };
    }
    
    private static class BoundedInputStream extends SubStream {
        
        private int bound;
        
        public BoundedInputStream(InputStream in, int bound) { super(in); this.bound = bound; }

        @Override
        public int read() throws IOException {
            if (bound > 0) {
                bound--;
                return in.read();
            } else {
                return -1;
            }
        }
        
        @Override
        public int read(byte[] buffer, int pos, int len)  throws IOException {
            if (bound > 0) {
                int advance = Math.min(len, bound);
                bound -= advance;
                return in.read(buffer, pos, advance);
            } else {
                return -1;
            }
        }
    }
        
    private class StreamIterator extends MessageIterator {
        
        private final InputStream input;
        private final Optional<Supplier<FeedPath>> nameSupplier;
        private Optional<Message> current;
        
        public StreamIterator(InputStream input, Optional<Supplier<FeedPath>> nameSupplier) {
            super(()->{
                try {
                    input.close();
                } catch(IOException e) {
                    throw FeedExceptions.runtime(e);
                }
            });
            this.input = input;
            this.nameSupplier = nameSupplier;
            moveNext();
        }
        
        private final void moveNext() {
            try {
                current = build(input, nameSupplier, false);
            } catch (FeedExceptions.BaseException e) {
                throw FeedExceptions.runtime(e);
            }         
        }

        @Override
        public boolean hasNext() {
            return current.isPresent();
        }

        @Override
        public Message next() {
            Message next = current.get();
            moveNext();
            return next;
        }
        
    }
    
    /** Default maximum size of the Json message header */
    public static final int MAX_HEADER_SIZE = 10000;
    
    /** Parse message headers from an input stream.
     * 
     * The input stream is not closed and after reading the header and the stream
     * should be positioned immediately after the closing } of the header object.
     * 
     * @param data
     * @return The json header, if one can be read. Empty if stream is ended.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidJson
     * @throws com.softwareplumbers.feed.FeedExceptions.StreamingException 
     */
    public static Optional<JsonObject> parseHeaders(InputStream data) throws FeedExceptions.InvalidJson, FeedExceptions.StreamingException {
        if (!data.markSupported()) throw new FeedExceptions.StreamingException("Mark not supported");
        data.mark(MAX_HEADER_SIZE);
        try {
            try (JsonParser parser = Json.createParser(new SubStream(data))) {
                if (parser.hasNext()) {
                    parser.next();
                    JsonObject result = parser.getObject();
                    data.reset();
                    data.skip(parser.getLocation().getStreamOffset());
                    return Optional.of(result);
                } else {
                    return Optional.empty();
                }
            } catch (JsonException e) {
                data.reset();
                byte[] buffer = new byte[128];
                int read = data.read(buffer);
                if (read >= 0) {
                    LOG.error("JSON parsing error: {}", e.getMessage());
                    String context = new String(Arrays.copyOf(buffer, read));
                    LOG.error("JSON context: {}", context);
                    throw(new FeedExceptions.InvalidJson(e.getMessage(), Optional.empty()));
                } else {
                    return Optional.empty();
                }
            }
        } catch (IOException e) {
            throw new FeedExceptions.StreamingException(e);
        }
    }
    
    /** Build a message from the given input stream.
     * 
     * @param data Input stream with header an binary data
     * @param nameSupplier Supplier of names for the parsed messages. This will override any names in the message headers.
     * @param temporary True if the message is only expected to be used once.
     * @return A message object.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidJson
     * @throws com.softwareplumbers.feed.FeedExceptions.StreamingException 
     */
    private Optional<Message> build(InputStream data, Optional<Supplier<FeedPath>> nameSupplier, boolean temporary) throws FeedExceptions.InvalidJson, FeedExceptions.StreamingException {
        if (!data.markSupported()) {
            data = new BufferedInputStream(data);
        }
        Optional<JsonObject> allHeaders = parseHeaders(data);
        
        if (allHeaders.isPresent()) {
            int length = Message.getLength(allHeaders.get());

            if (length >= 0) {
                data = new BoundedInputStream(data, length);
            }
            
            FeedPath name = nameSupplier.isPresent() ? nameSupplier.get().get() : Message.getName(allHeaders.get()).orElse(FeedPath.ROOT);
            String sender = Message.getSender(allHeaders.get()).orElse(null);
            Instant timestamp = Message.getTimestamp(allHeaders.get()).orElse(null);
            UUID serverId = Message.getServerId(allHeaders.get()).orElse(null);

            return Optional.of(
                new MessageImpl(
                    name, 
                    sender,
                    timestamp, 
                    serverId,
                    Message.getHeaders(allHeaders.get()), 
                    data, 
                    length, 
                    temporary
                )
            );
        } else {
            return Optional.empty();
        }
    }
    
    public Optional<Message> build(InputStream data, boolean temporary) throws FeedExceptions.InvalidJson, FeedExceptions.StreamingException {
        return build(data, Optional.empty(), temporary);
    }
    
    public Optional<Message> build(InputStream data, Supplier<FeedPath> names, boolean temporary)  throws FeedExceptions.InvalidJson, FeedExceptions.StreamingException  {
        return build(data, Optional.of(names), temporary);
    }
    
    public void consume(InputStream data, Consumer<Message> messageConsumer, Optional<Supplier<FeedPath>> names)  throws FeedExceptions.InvalidJson, FeedExceptions.StreamingException  {
        Optional<Message> message;
        while ((message = build(data, names, false)).isPresent()) messageConsumer.accept(message.get());
    }
    
    public MessageIterator buildIterator(InputStream data, Optional<Supplier<FeedPath>> names) {
        return new StreamIterator(data, names);
    }
        
}
