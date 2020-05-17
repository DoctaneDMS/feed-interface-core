/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.impl.MessageFactory;
import static com.softwareplumbers.feed.test.TestUtils.asString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static com.softwareplumbers.feed.test.TestUtils.*;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author jonathan
 */
public class TestMessageFactory {
    
    @Test
    public void testParseFromStream() throws IOException {
        MessageFactory factory = new MessageFactory();
        InputStream data = new ByteArrayInputStream("{ \"headers\": { \"a\" : \"one\" } }plus some more unstructured data".getBytes());
        Message message = factory.build(data, ()->FeedPath.ROOT.addId("test"), false).orElseThrow(()->new RuntimeException("no message"));
        assertEquals(FeedPath.ROOT.addId("test"), message.getName());
        assertThat(message.getTimestamp(), lessThanOrEqualTo(Instant.now()));
        assertThat(asString(message.getData()), equalTo("plus some more unstructured data"));
        assertThat(asString(message.getData()), equalTo("plus some more unstructured data"));
    }
    
    @Test 
    public void testParseTempFromStream() throws IOException {
        MessageFactory factory = new MessageFactory();
        InputStream data = new ByteArrayInputStream("{ \"headers\": { \"a\" : \"one\" } }plus some more unstructured data".getBytes());
        Message message = factory.build(data, ()->FeedPath.ROOT.addId("test"), true).orElseThrow(()->new RuntimeException("no message"));
        assertEquals(FeedPath.ROOT.addId("test"), message.getName());
        assertEquals("one", message.getHeaders().getString("a"));
        assertThat(message.getTimestamp(), lessThanOrEqualTo(Instant.now()));
        assertThat(asString(message.getData()), equalTo("plus some more unstructured data"));
        //Can only use stream once
        assertThat(asString(message.getData()), equalTo(""));
    }   
    
    @Test
    public void testIteratorFromStream() throws IOException {
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Map<FeedPath, Message> messages = generateBinaryMessageStream(20, bos);
        
        MessageIterator iterator = new MessageFactory().buildIterator(new ByteArrayInputStream(bos.toByteArray()), Optional.empty());
        int count = 0;
        while (iterator.hasNext()) {
            count++;
            Message next = iterator.next();
            assertThat(messages, hasEntry(next.getName(), next));
            assertThat(asString(next.getData()), equalTo(asString(messages.get(next.getName()).getData())));
        }
        assertThat(count, equalTo(messages.size()));
    }
    
    @Test
    public void testConsumerFromStream() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Map<FeedPath, Message> messages = generateBinaryMessageStream(20, bos);
        new MessageFactory().consume(new ByteArrayInputStream(bos.toByteArray()), message->{
            assertThat(messages, hasEntry(message.getName(), message));
            assertThat(asString(message.getData()), equalTo(asString(messages.get(message.getName()).getData())));
        }, Optional.empty());
    }
}
