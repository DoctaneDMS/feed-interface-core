/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

/** Exception classes for Feed services.
 *
 * @author jonathan
 */
public class FeedExceptions {
    
    public static enum Type {
        INVALID_PATH,
        INVALID_JSON
    }
    
    /** Base exception.
     * 
     * All checked Feed exceptions will be a subclass.
     * 
     */
    public static class BaseException extends Exception {
        public final Type type;
        public BaseException(Type type, String reason) {
            super(reason);
            this.type = type;
        }
        public static Optional<Type> getType(JsonObject obj) {
            try {
                return Optional.of(Type.valueOf(obj.getString("type")));
            } catch (Exception e) {
                return Optional.empty();
            }
        }
        public JsonObjectBuilder buildJson(JsonObjectBuilder bldr) {
            return bldr.add("type", type.toString());
        }
        public JsonObject toJson() {
            return buildJson(Json.createObjectBuilder()).build();
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
    public static class InvalidJson extends BaseException {
        public final JsonObject json;
        public InvalidJson(JsonObject json) {
            super(Type.INVALID_JSON, "Invalid Json: " + json.toString());
            this.json = json;
        }
        public static Optional<JsonObject> getJson(JsonObject object) {
            try {
                return Optional.of(object.getJsonObject("json"));
            } catch (Exception e) {
                return Optional.empty();
            }
        }
        public JsonObjectBuilder buildJson(JsonObjectBuilder bldr) {
            return super.buildJson(bldr).add("json", json);
        }
    }

    /** Throw if a path is invalid
     * 
     */
    public static class InvalidPath extends BaseException {
        public final FeedPath path;
        public InvalidPath(FeedPath path) {
            super(Type.INVALID_PATH, "Invalid Path: " + path.toString());
            this.path = path;
        }
        public static Optional<FeedPath> getPath(JsonObject object) {
            try {
                return Optional.of(FeedPath.valueOf(object.getString("path")));
            } catch (Exception e) {
                return Optional.empty();
            }
        }
        public JsonObjectBuilder buildJson(JsonObjectBuilder bldr) {
            return super.buildJson(bldr).add("path", path.toString());
        }
    }
    
    public static BaseException build(JsonObject json) throws InvalidJson {
        Type type = BaseException.getType(json).orElseThrow(()->new InvalidJson(json));
        switch(type) {
            case INVALID_PATH:
                return new InvalidPath(InvalidPath.getPath(json).orElseThrow(()->new InvalidJson(json)));
            case INVALID_JSON:
                return new InvalidJson(InvalidJson.getJson(json).orElseThrow(()->new InvalidJson(json)));
            default:
                throw new InvalidJson(json);
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
