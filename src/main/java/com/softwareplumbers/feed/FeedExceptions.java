/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** Exception classes for Feed services.
 *
 * @author jonathan
 */
public class FeedExceptions {
    
    /** Base exception.
     * 
     * All checked Feed exceptions will be a subclass.
     * 
     */
    public static class BaseException extends Exception {
        public BaseException(String reason) {
            super(reason);
        }
    }
    
    public static class BaseRuntimeException extends RuntimeException {
        public BaseRuntimeException(BaseException e) {
            super(e);
        }
        
    }
    
    /** Throw if a path is invalid
     * 
     */
    public static class InvalidPath extends BaseException {
        public final FeedPath path;
        public InvalidPath(FeedPath path) {
            super("Invalid Path: " + path.toString());
            this.path = path;
        }
    }
    
    @FunctionalInterface
    public static interface CheckedConsumer<T> {
        void accept(T t) throws BaseException;
    }

    @FunctionalInterface
    public static interface CheckedBiConsumer<T,U> {
        void accept(T t, U u) throws BaseException;
    }
    
    public static <T> Consumer<T> runtime(final CheckedConsumer<T> consumer) {
        return t->{
            try {
                consumer.accept(t);
            } catch (BaseException e) {
                throw new BaseRuntimeException(e);
            }
        };
    }

    public static <T,U> BiConsumer<T,U> runtime(final CheckedBiConsumer<T,U> consumer) {
        return (t,u)->{
            try {
                consumer.accept(t,u);
            } catch (BaseException e) {
                throw new BaseRuntimeException(e);
            }
        };
    }    
}
