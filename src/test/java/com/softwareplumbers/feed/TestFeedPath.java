/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidPathSyntax;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import org.junit.Test;

/**
 *
 * @author jonathan
 */
public class TestFeedPath {
    
    @Test
    public void testSimplePath() throws InvalidPathSyntax {
        FeedPath path = FeedPath.ROOT.add("abc").add("def").addId("123");
        assertThat(FeedPath.valueOf(path.toString()), equalTo(path));
    }
    
    @Test
    public void testSimplePathWithEscape() throws InvalidPathSyntax {
        FeedPath path = FeedPath.valueOf("/xyz\\/abc/234");
        assertThat(path, equalTo(FeedPath.ROOT.add("xyz/abc").add("234")));
    }    

    @Test
    public void testSimplePathWithNonStandardEscape() throws InvalidPathSyntax {
        FeedPath path = FeedPath.valueOf("/xyz+/abc/234",'+');
        assertThat(path, equalTo(FeedPath.ROOT.add("xyz/abc").add("234")));
    }    
    
    @Test
    public void testSimplePathWithRootId() throws InvalidPathSyntax {
        FeedPath path = FeedPath.valueOf("~xyz/abc/234");
        assertThat(path, equalTo(FeedPath.ROOT.addId("xyz").add("abc").add("234")));
    }
    
    @Test
    public void testBeforeMessageId() {
        FeedPath path = FeedPath.ROOT.add("abc").add("def");
        FeedPath pathWithId = path.addId("123");
        assertThat(pathWithId.beforeMessageId(), equalTo(path));        
        assertThat(path.beforeMessageId(), equalTo(path));        
    }
}
