/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 *
 * @author jonathan
 */
public class TestFeedPath {
    
    @Test
    public void testSimplePath() {
        FeedPath path = FeedPath.ROOT.add("abc").add("def").addId("123");
        assertEquals(path, FeedPath.valueOf(path.toString()));
    }
    
}
