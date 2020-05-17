/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import org.junit.Test;

/**
 *
 * @author jonathan
 */
public class TestFeedPath {
    
    @Test
    public void testSimplePath() {
        FeedPath path = FeedPath.ROOT.add("abc").add("def").addId("123");
        assertThat(FeedPath.valueOf(path.toString()), equalTo(path));
    }
    
    @Test
    public void testBeforeMessageId() {
        FeedPath path = FeedPath.ROOT.add("abc").add("def");
        FeedPath pathWithId = path.addId("123");
        assertThat(pathWithId.beforeMessageId(), equalTo(path));        
        assertThat(path.beforeMessageId(), equalTo(path));        
    }
}
