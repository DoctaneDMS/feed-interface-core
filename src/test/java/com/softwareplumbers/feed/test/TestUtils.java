/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.test;

import com.softwareplumbers.common.pipedstream.OutputStreamConsumer;
import com.softwareplumbers.feed.FeedPath;
import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.impl.MessageImpl;
import com.softwareplumbers.feed.impl.buffer.MessageBuffer;
import com.softwareplumbers.feed.impl.buffer.MessageClock;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import javax.json.Json;
import javax.json.JsonObject;

/**
 *
 * @author jonathan
 */
public class TestUtils {
    
    public static String asString(InputStream is) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        OutputStreamConsumer.of(()->is).consume(stream);
        return stream.toString();
    }

    private static final String[] WORDS = new String[] { "sphagetti", "idle", "loves", "jane", "dog", "hair", "tantric", "slightly", "worm", "likely", "moves", "gets", "fast" };
    
    private static final FeedPath[] FEEDS = new FeedPath[] {
        FeedPath.valueOf("accounts/teamA/baseball"),
        FeedPath.valueOf("accounts/teamA/audit"),
        FeedPath.valueOf("marketing/poker"),
        FeedPath.valueOf("sales/ringthebell")  
    };
    
    private static String randomText(int count) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            builder.append(WORDS[(int)(Math.random() * WORDS.length)]);
            builder.append(" ");
        }
        return builder.toString();
    } 
    
    private static FeedPath randomFeedPath() {
        return FEEDS[(int)(Math.random() * FEEDS.length)];
    }
    
    public static final MessageClock CLOCK = new MessageClock();
    
    public static Message generateMessage() {
        JsonObject testHeaders = Json.createObjectBuilder()
            .add("field1", randomText(1))
            .add("field2", randomText(1)).build();
        InputStream testData = new ByteArrayInputStream(randomText(10).getBytes());
        Instant time = Instant.now(CLOCK);
        FeedPath id = randomFeedPath().addId(UUID.randomUUID().toString());
        try {
            return new MessageImpl(id, time, testHeaders, testData, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static int getAverageMessageSize() throws IOException {
        byte[] buffer = new byte[10000];
        int pos = 0;
        for (int i = 0; i < 10; i ++) {
            pos += generateMessage().toStream().read(buffer, pos, buffer.length - pos);
        }
        return pos/10;    
    }
    
    public static void randomPause(int maxPause) {
        int pause = (int)(Math.random() * maxPause);
        if (pause > 0) {
            try {
                Thread.sleep(pause); // Make sure messages have different timestamps
            } catch (InterruptedException e) {
                // don't care
            }
        }
    }
    
    public static NavigableMap<FeedPath,Message> generateMessages(int count, int maxPause, Consumer<Message> messageConsumer) {
        NavigableMap<FeedPath,Message> result = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            Message message = generateMessage();
            result.put(message.getName(), message);
            messageConsumer.accept(message);
            randomPause(maxPause);
        }
        return result;
    }
    
    public static NavigableMap<FeedPath,Message> generateMessages(int threads, int count, int maxPause, Consumer<Message> messageConsumer) {
        ConcurrentSkipListMap<FeedPath,Message> result = new ConcurrentSkipListMap<>();
        for (int i = 0; i < threads; i++) {
            new Thread(() -> {
                for (int j = 0; j < count; j++) {
                    Message message = generateMessage();
                    result.put(message.getName(), message);
                    messageConsumer.accept(message);
                    randomPause(maxPause);
                }
            }).start();
        }
        return result;
    }  
    
    public static List<FeedPath> getFeeds() {
        return Collections.unmodifiableList(Arrays.asList(FEEDS));
    }

}
