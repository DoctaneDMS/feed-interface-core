/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.test;

import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author jonat
 */
public class TestDumpTreads {
    @Test
    public void testDump() {
        TestUtils.dumpThreads();
    }
}
