/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import static com.softwareplumbers.feed.test.TestUtils.*;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import org.junit.Test;

/**
 *
 * @author jonathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { LocalConfig.class })
public class TestFeedService {

    @Autowired
    FeedService service;
    
    public void post(Message message) {
        try {
            service.post(message.getFeedName(), message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @Test
    public void testMessageRoundtripSingleThread() throws IOException, InvalidPath {
        
        NavigableMap<FeedPath, Message> sentMessages = generateMessages(1000, 2, this::post); 
        List<FeedPath> feeds = getFeeds();
        service.dumpState();
        assertThat(sentMessages.size(), equalTo(1000));
        TreeMap<FeedPath, Message> responseMessages = new TreeMap<>();
        int count = 0;
        for (FeedPath feed : feeds) {
            try (MessageIterator messages = service.sync(feed, Instant.EPOCH)) {
                while (messages.hasNext()) {
                    Message received = messages.next();
                    responseMessages.put(received.getName(), received);
                    assertThat(received.getName(), isIn(sentMessages.keySet()));
                    Message sent = sentMessages.get(received.getName());
                    assertThat(received, equalTo(sent));
                    assertThat(asString(received.getData()), equalTo(asString(sent.getData())));
                    count++;
                }
            }
        }
        for (FeedPath message : sentMessages.keySet()) {
            if (!responseMessages.containsKey(message))
                System.out.println("missing: " + message);
            //assertThat(message, isIn(responseMessages.keySet()));
        }
        assertThat(count, equalTo(1000));        
    }
    
}
