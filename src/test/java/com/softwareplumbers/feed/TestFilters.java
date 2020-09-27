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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
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
}
