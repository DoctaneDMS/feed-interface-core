/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

/**
 *
 * @author jonathan
 */
public class FeedExceptions {
    
    public static class BaseException extends Exception {
        public BaseException(String reason) {
            super(reason);
        }
    }
    
    public static class InvalidPath extends BaseException {
        public final FeedPath path;
        public InvalidPath(FeedPath path) {
            super("Invalid Path: " + path.toString());
            this.path = path;
        }
    }
}
