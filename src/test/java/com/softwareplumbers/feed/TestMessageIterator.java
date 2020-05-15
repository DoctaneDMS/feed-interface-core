/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import static com.softwareplumbers.feed.test.TestUtils.generateMessages;
import static com.softwareplumbers.feed.test.TestUtils.getFeeds;
import static com.softwareplumbers.feed.test.TestUtils.randomFeedPath;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import org.junit.Test;

/**
 *
 * @author jonathan
 */
public class TestMessageIterator {
    @Test
    public void testSequence() {
        Map<FeedPath,Message> messages1 = generateMessages(3,2,randomFeedPath(),m->{});
        Map<FeedPath,Message> messages2 = generateMessages(4,2,randomFeedPath(),m->{});
        Map<FeedPath,Message> messages3 = generateMessages(5,2,randomFeedPath(),m->{});
        
        MessageIterator seq = MessageIterator.of(
            MessageIterator.of(messages1.values().iterator(), ()->{}),
            MessageIterator.of(messages2.values().iterator(), ()->{}),
            MessageIterator.of(messages3.values().iterator(), ()->{})
        );
        
        Set<FeedPath> received = new TreeSet<>();
        while (seq.hasNext()) received.add(seq.next().getName());
        
        for (FeedPath message : messages1.keySet()) {
            assertThat(received, hasItem(message));
        }

        for (FeedPath message : messages2.keySet()) {
            assertThat(received, hasItem(message));
        }
        
        for (FeedPath message : messages3.keySet()) {
            assertThat(received, hasItem(message));
        }
    }
}
