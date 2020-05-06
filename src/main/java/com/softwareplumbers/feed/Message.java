/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import javax.json.JsonObject;

/**
 *
 * @author jonathan
 */
public interface Message extends AutoCloseable {
    
    public JsonObject header();
    public InputStream getData();
    public void writeData(OutputStream out);
    public FeedPath getName();
}
