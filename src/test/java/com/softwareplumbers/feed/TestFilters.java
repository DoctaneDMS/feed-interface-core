/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.Filters.RemotablePredicate;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import javax.json.JsonArray;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import org.hamcrest.Matchers;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 * @author jonat
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { LocalConfig.class })
public class TestFilters {
    
    @Autowired @Qualifier(value="testSimpleClusterNodeA")
    protected FeedService service;    

    @Autowired @Qualifier(value="testSimpleClusterNodeB")
    protected FeedService remoteService;    

    @Test
    public void testAckFilter() {
        Predicate<Message> filter = Filters.IS_ACK;
        assertThat(filter, instanceOf(RemotablePredicate.class));
        RemotablePredicate remotable = (RemotablePredicate)filter;
        assertThat(Filters.using(service).fromJson(remotable.toJson()).get(), is(Filters.IS_ACK));
    }

    @Test
    public void testNAckFilter() {
        Predicate<Message> filter = Filters.NO_ACKS;
        assertThat(filter, instanceOf(RemotablePredicate.class));
        RemotablePredicate remotable = (RemotablePredicate)filter;
        assertThat(Filters.using(service).fromJson(remotable.toJson()).get(), is(Filters.NO_ACKS));
    }

    @Test
    public void testPostedLocallyFilter() {
        Predicate<Message> filter = Filters.POSTED_LOCALLY;
        assertThat(filter, instanceOf(RemotablePredicate.class));
        RemotablePredicate remotable = (RemotablePredicate)filter;
        assertThat(Filters.using(service).fromJson(remotable.toJson()).get(), is(Filters.POSTED_LOCALLY));
    }
    
    @Test
    public void testFromRemoteFilter() {
        UUID test = UUID.randomUUID();
        Predicate<Message> filter = Filters.fromRemote(test);
        assertThat(filter, instanceOf(RemotablePredicate.class));
        RemotablePredicate remotable = (RemotablePredicate)filter;
        assertThat(Filters.using(service).fromJson(remotable.toJson()).get(), equalTo(Filters.fromRemote(test)));
    }

    @Test
    public void testFromByRemoteTimestamp() {
        UUID test = remoteService.getServerId();
        Instant now = Instant.now();
        Predicate<Message> filter = Filters.using(service).byRemoteTimestamp(test, now, Optional.empty());
        assertThat(filter, instanceOf(RemotablePredicate.class));
        RemotablePredicate remotable = (RemotablePredicate)filter;
        assertThat(Filters.using(service).fromJson(remotable.toJson()).get(), equalTo(Filters.using(service).byRemoteTimestamp(test, now, Optional.empty())));
    }  
    
    @Test 
    public void testRemoteList() {
        JsonArray result = Filters.toJson(new Predicate[] { Filters.IS_ACK, Filters.POSTED_LOCALLY, message->true });
        assertThat(result, Matchers.iterableWithSize(2));
        assertThat(Filters.using(service).fromJson(result.getJsonObject(0)).get(), is(Filters.IS_ACK));
        assertThat(Filters.using(service).fromJson(result.getJsonObject(1)).get(), is(Filters.POSTED_LOCALLY));
    }
    
    @Test 
    public void testParseRemoteList() {
        JsonArray result = Filters.toJson(new Predicate[] { Filters.IS_ACK, Filters.POSTED_LOCALLY, message->true });
        assertThat(result, Matchers.iterableWithSize(2));
        assertThat(Filters.using(service).fromJson(result)[0], is(Filters.IS_ACK));
        assertThat(Filters.using(service).fromJson(result)[1], is(Filters.POSTED_LOCALLY));
    }
    
    @Test 
    public void testLocalList() {
        Predicate<Message>[] result = Filters.local(new Predicate[] { Filters.IS_ACK, Filters.POSTED_LOCALLY, message->true });
        assertThat(result, Matchers.arrayWithSize(1));
        assertTrue(result[0].test(null));
    }    
}
