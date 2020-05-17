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
import com.softwareplumbers.feed.impl.MessageImpl;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.time.Instant;
import java.util.TreeMap;
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
        timeIndex.put(message.getTimestamp(), buffered);
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
