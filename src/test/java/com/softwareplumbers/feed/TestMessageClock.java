/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.impl.buffer.MessageClock;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 *
 * @author jonathan
 */
public class TestMessageClock {
    
    @Test
    public void testAllValuesDistinct() {
        Instant[] results = new Instant[1000];
        MessageClock clock = new MessageClock();
        for (int i = 0; i < 1000; i++) results[i] = Instant.now(clock);
        HashSet<Instant> resultSet = new HashSet<>();
        resultSet.addAll(Arrays.asList(results));
        assertEquals(resultSet.size(), 1000);
    }
    
    @Test
    public void testMoreOrLessInSyncWithSystemClock() throws InterruptedException {
        Instant[] results = new Instant[10];
        MessageClock clock = new MessageClock();
        Instant first = Instant.now();
        Thread.sleep(100);
        for (int i = 0; i < 10; i++) results[i] = clock.instant();
        Thread.sleep(100);
        Instant last = Instant.now();
        for (int i = 0; i < 10; i++) {
            assertThat(results[i], greaterThan(first));            
            assertThat(results[i], lessThan(last));            
        }
    }
    
}
