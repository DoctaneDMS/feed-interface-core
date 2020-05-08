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
import java.io.OutputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonWriter;

/**
 *
 * @author jonathan
 */
class Bucket {
    
    private byte[] buffer;
    private int position;
    
    private final TreeMap<Instant, List<Message>> timeIndex;
    
    private OutputStream out = new OutputStream() {
        @Override
        public void write(int b) throws IOException {
            if (position < buffer.length) {
                buffer[position++] = (byte) b;
            } else {
                throw new IOException("Buffer overflow");
            }
        }
    };

    private InputStreamSupplier chunk(int from, int to) {
        return InputStreamSupplier.markPersistent(() -> new InputStream() {
            int position = from;

            @Override
            public int read() throws IOException {
                if (position < to) {
                    return buffer[position++];
                } else {
                    return -1;
                }
            }
        });
    }

    public Bucket(int maxSize) {
        buffer = new byte[maxSize];
        position = 0;
        timeIndex = new TreeMap<>();
    }
    
    /** Resize a bucket.
     * 
     * Use BucketPool.resize() rather than this method.
     * 
     * @param maxSize 
     */
    void resize(int maxSize) {
        byte[] old = buffer;
        buffer = new byte[maxSize];
        System.arraycopy(old, 0, buffer, 0, position);
    }
    
    public int size() {
        return buffer.length;
    }
    

    public Message addMessage(FeedPath path, Instant time, JsonObject allHeaders, InputStream data) throws IOException, MessageOverflow, HeaderOverflow {
        String id = path.part.getId().orElseThrow(() -> new IllegalArgumentException("Invalid feed path"));
        int start = position;
        try (final JsonWriter writer = Json.createWriter(out)) {
            writer.write(allHeaders);
        } catch (JsonException exp) {
            throw new HeaderOverflow(position - start);
        }
        int count = data.read(buffer, position, buffer.length - position);
        if (count < 0) {
            throw new IOException("unexpected end of stream");
        }
        position += count;
        if (position == buffer.length) {
            throw new MessageOverflow(position - start, chunk(start, position));
        }
        Message message = new BufferedMessageImpl(chunk(start, position));
        timeIndex.computeIfAbsent(time, (k) -> new LinkedList<>()).add(message);
        return message;
    }

    protected Message addMessage(FeedPath path, Instant time, InputStream overflow, InputStream data) throws IOException, MessageOverflow {
        String id = path.part.getId().orElseThrow(() -> new IllegalArgumentException("Invalid feed path"));
        int start = position;
        int count = overflow.read(buffer, position, buffer.length - position);
        if (count < 0) {
            throw new IOException("unexpected end of stream");
        }
        position += count;
        if (position == buffer.length) {
            throw new IOException("insufficient buffer for overflow");
        }
        count = data.read(buffer, position, buffer.length - position);
        if (count < 0) {
            throw new IOException("unexpected end of stream");
        }
        position += count;
        if (position == buffer.length) {
            throw new MessageOverflow(position - start, chunk(start, position));
        }
        Message message = new BufferedMessageImpl(chunk(start, position));
        timeIndex.computeIfAbsent(time, (k) -> new LinkedList<>()).add(message);
        return message;
    }

    void dumpBucket() {
        for (Instant time : timeIndex.keySet()) {
            System.out.println(time + ":" + timeIndex.get(time).stream().map(Message::toString).collect(Collectors.joining(",")));
        }
    }
    
    Instant firstTimestamp() {
        return timeIndex.firstKey();
    }
    
    boolean isEmpty() {
        return timeIndex.isEmpty();
    }
    
    Stream<Message> getMessages() {
        return timeIndex.values().stream().flatMap(List::stream);
    }
    
    Stream<Message> getMessagesAfter(Instant timestamp) {
        return timeIndex.tailMap(timestamp).values().stream().flatMap(List::stream);
    }
    
}
