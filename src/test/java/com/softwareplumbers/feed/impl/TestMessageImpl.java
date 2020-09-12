/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.Message;
import com.softwareplumbers.feed.Message.RemoteInfo;
import com.softwareplumbers.feed.MessageType;
import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
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
        
        MessageImpl impl = new MessageImpl(MessageType.NONE, null, null, null, Optional.empty(), Optional.empty(), JsonValue.EMPTY_JSON_OBJECT, new ByteArrayInputStream(NO_BYTES), -1, false);
        assertThat(impl.getName(), nullValue());
        assertThat(impl.getId(), nullValue());
        assertThat(impl.getFeedName(), nullValue());
        assertThat(impl.getHeaders(), equalTo(JsonValue.EMPTY_JSON_OBJECT));
        assertThat(impl.getSender(), nullValue());
        assertThat(impl.getServerId(), equalTo(Optional.empty()));

    }
    
    @Test
    public void testCanWriteMinimalMessage() {
        MessageImpl impl = new MessageImpl(MessageType.NONE, null, null, null, Optional.empty(), Optional.empty(), JsonValue.EMPTY_JSON_OBJECT, new ByteArrayInputStream(NO_BYTES), -1, false);
        JsonObject headerStream = Json.createReader(impl.getHeaderStream()).readObject();
        assertThat(headerStream, equalTo(MIN_HEADERS));        
    }
    
    @Test
    public void testRemoteInfo() {
        UUID remoteServerId = UUID.randomUUID();
        Instant now = Instant.now();
        Message remote = new MessageImpl(MessageType.NONE, null, null, now, Optional.of(remoteServerId), Optional.empty(), JsonValue.EMPTY_JSON_OBJECT, new ByteArrayInputStream(NO_BYTES), -1, false);
        Instant later = now.plusMillis(10);
        UUID localServerId = UUID.randomUUID();
        RemoteInfo remoteInfo = new RemoteInfo(remoteServerId, now);
        Message simple = new MessageImpl(MessageType.NONE, null, null, later, Optional.of(localServerId), Optional.of(remoteInfo), JsonValue.EMPTY_JSON_OBJECT, new ByteArrayInputStream(NO_BYTES), -1, false);
        assertThat(simple.getRemoteInfo().get().serverId, equalTo(remoteServerId));
        assertThat(simple.getRemoteInfo().get().timestamp, equalTo(now));
        assertThat(simple.getRemoteInfo().get(), equalTo(remoteInfo));
        Message updated = remote.localizeTimestamp(localServerId, later);
        assertThat(updated.getServerId(), equalTo(Optional.of(localServerId)));
        assertThat(updated.getTimestamp(), equalTo(later));
        assertThat(updated.getRemoteInfo().get().serverId, equalTo(remoteServerId));
        assertThat(updated.getRemoteInfo().get().timestamp, equalTo(now));
        assertThat(updated.getRemoteInfo().get(), equalTo(remoteInfo));
    }
        
    
}
