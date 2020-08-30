/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.test.TestUtils;
import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import org.junit.Test;

/**
 *
 * @author jonathan
 */
public class TestMessageImpl {
    
    public static final byte[] NO_BYTES = new byte[0];
    
    public static final JsonObject MIN_HEADERS = Json.createObjectBuilder()
        .add("headers", JsonValue.EMPTY_JSON_OBJECT)
        .add("length", 0)
        .build();
    
    @Test
    public void testCanCreateMinimalMessage() {
        
        MessageImpl impl = new MessageImpl(null, null, null, null, JsonValue.EMPTY_JSON_OBJECT, new ByteArrayInputStream(NO_BYTES), -1, false);
        assertThat(impl.getName(), nullValue());
        assertThat(impl.getId(), nullValue());
        assertThat(impl.getFeedName(), nullValue());
        assertThat(impl.getHeaders(), equalTo(JsonValue.EMPTY_JSON_OBJECT));
        assertThat(impl.getSender(), nullValue());
        assertThat(impl.getServerId(), nullValue());

    }
    
    @Test
    public void testCanWriteMinimalMessage() {
        MessageImpl impl = new MessageImpl(null, null, null, null, JsonValue.EMPTY_JSON_OBJECT, new ByteArrayInputStream(NO_BYTES), -1, false);
        JsonObject headerStream = Json.createReader(impl.getHeaderStream()).readObject();
        assertThat(headerStream, equalTo(MIN_HEADERS));        
    }
    
}
