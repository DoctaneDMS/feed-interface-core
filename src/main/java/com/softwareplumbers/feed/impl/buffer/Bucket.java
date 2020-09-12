/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.feed.FeedExceptions;
import com.softwareplumbers.feed.FeedExceptions.StreamingException;
import com.softwareplumbers.feed.Message;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.SequenceInputStream;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

/**
 *
 * @author jonathan
 */
class Bucket {
    
    @FunctionalInterface
    public interface OverflowHandler {
        public Message recover(Message message, int size) throws StreamingException;
    }
    
    private byte[] buffer;
    private int position;
    
    private final NavigableMap<Instant, List<Message>> timeIndex;
    private final Map<String, List<Message>> idIndex;
    
    private class BufferOverflow extends IOException {
        
    }
    
    private OutputStream out = new OutputStream() {
        @Override
        public void write(int b) throws BufferOverflow {
            if (position < buffer.length) {
                buffer[position++] = (byte) b;
            } else {
                throw new BufferOverflow();
            }
        }
        
        @Override
        public void write(byte[] input, int pos, int length) throws BufferOverflow {
            if (position + length <  buffer.length) {
                System.arraycopy(input, pos, buffer, position, length);
                position += length;
            } else {
                throw new BufferOverflow();
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
        timeIndex = new ConcurrentSkipListMap<>();
        idIndex = new ConcurrentHashMap<>();
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
    

    public Message addMessage(Message message, OverflowHandler handler) throws FeedExceptions.StreamingException {
        int start = position;
        Message recovered = message.writeData(out, (e,is)->recoveryHandler(message, e, start, is, handler));
        if (recovered != null) return recovered;
        int endData = position;
        try {
            message.writeHeaders(out);
        } catch (StreamingException e) {
            if (e.getCause() instanceof BufferOverflow) {
                recovered = message.setData(chunk(start,endData), -1);
                return handler.recover(recovered, start-position);
            }
        }
        
        Message buffered = new BufferedMessageImpl(chunk(endData, position), chunk(start, endData));
        timeIndex.computeIfAbsent(message.getTimestamp(), key->new LinkedList<>()).add(buffered);
        idIndex.computeIfAbsent(message.getId(), key->new LinkedList()).add(message);
        return buffered;
    }
    
    private Message recoveryHandler(Message message, IOException e, int start, InputStream recoveredData, OverflowHandler overflowHandler) throws StreamingException {
        if (e instanceof BufferOverflow) {
            if (recoveredData != null) {
                message = message.setData(()->new SequenceInputStream(chunk(start, position).get(), recoveredData), -1);
            } 
            return overflowHandler.recover(message, start-position);
        } else {
            throw new StreamingException(e);
        }
    }

    void dumpBucket(PrintWriter out) {
        for (Instant time : timeIndex.keySet()) {
            out.println(time + ":" + timeIndex.get(time));
        }
    }
    
    Optional<Instant> firstTimestamp() {
        return timeIndex.isEmpty() ? Optional.empty() : Optional.of(timeIndex.firstKey());
    }
    
    boolean isEmpty() {
        return timeIndex.isEmpty();
    }
    
    Stream<Message> getMessages() {
        return timeIndex.values().stream().flatMap(entries->entries.stream());
    }
    
    Stream<Message> getMessagesAfter(Instant timestamp) {
        return timeIndex.tailMap(timestamp, false).values().stream().flatMap(entries->entries.stream());
    }

    /** Get messages between from and to.
     * 
     * @param from Returns messages with timestamp greater than from
     * @param fromInclusive includes any messages with timestamp equal to from
     * @param to Returns messages with timestamp less than or equal to to
     * @param toInclusive includes any messages with timestamp equal to to
     * @return Stream of messages.
     */
    Stream<Message> getMessagesBetween(Instant from, boolean fromInclusive, Instant to, boolean toInclusive) {
        return timeIndex.subMap(from, fromInclusive, to, toInclusive).values().stream().flatMap(entries->entries.stream());
    }
    
    Stream<Message> getMessages(String id) {
        return idIndex.getOrDefault(id, Collections.EMPTY_LIST).stream();
    }
        
}
