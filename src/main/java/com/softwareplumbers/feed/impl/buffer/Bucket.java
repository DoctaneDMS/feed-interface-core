/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import com.softwareplumbers.common.pipedstream.InputStreamSupplier;
import com.softwareplumbers.feed.Message;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.time.Instant;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 *
 * @author jonathan
 */
class Bucket {
    
    @FunctionalInterface
    public interface OverflowHandler {
        public Message recover(InputStream recovered, int currentSize) throws IOException;
    }
    
    private byte[] buffer;
    private int position;
    
    private final TreeMap<Instant, Message> timeIndex;
    
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
    

    public Message addMessage(Message message, OverflowHandler handler) throws IOException {
        int start = position;
        Message recovered = message.write(out, (e,is)->recoveryHandler(e, start, is, handler));
        if (recovered != null) return recovered;
        Message buffered = new BufferedMessageImpl(chunk(start, position));
        timeIndex.put(message.getTimestamp(), buffered);
        return buffered;
    }
    
    public Message addMessage(Instant timestamp, InputStream recovered, OverflowHandler handler) throws IOException {
        int start = position;
        int count;
        while ((count = recovered.read(buffer, position, buffer.length - position)) >= 0 && position < buffer.length) {
            position += count;
        }
        if (count < 0) {
            Message buffered = new BufferedMessageImpl(chunk(start, position));
            timeIndex.put(timestamp, buffered);
            return buffered;
        } else {
            return handler.recover(recovered, position-start);
        }
    }

    private Message recoveryHandler(IOException e, int start, InputStream recovered, OverflowHandler overflowHandler) throws IOException {
        if (e instanceof BufferOverflow) {
            recovered = recovered == null ? null : new SequenceInputStream(chunk(start, position).get(), recovered);
            return overflowHandler.recover(recovered, position - start);
        } else {
            throw e;
        }
    }

    void dumpBucket() {
        for (Instant time : timeIndex.keySet()) {
            System.out.println(time + ":" + timeIndex.get(time));
        }
    }
    
    Instant firstTimestamp() {
        return timeIndex.firstKey();
    }
    
    boolean isEmpty() {
        return timeIndex.isEmpty();
    }
    
    Stream<Message> getMessages() {
        return timeIndex.values().stream();
    }
    
    Stream<Message> getMessagesAfter(Instant timestamp) {
        return timeIndex.tailMap(timestamp, false).values().stream();
    }
    
}
