/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl.buffer;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

/**
 *
 * @author jonathan
 */
public class MessageClock extends Clock {
    
    ZoneId zoneId;
    
    long nanos = 0;
    long lastSystemTime;

    @Override
    public ZoneId getZone() {
        return ZoneId.systemDefault();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Instant instant() {
        long nextSystemTime = System.currentTimeMillis();
        synchronized (this) {
            if (nextSystemTime <= lastSystemTime) {
                nanos++;
            } else {
                lastSystemTime = nextSystemTime;
                nanos = 0;
            }
            return Instant.ofEpochMilli(lastSystemTime).plusNanos(nanos);
        }
    }
    
}
