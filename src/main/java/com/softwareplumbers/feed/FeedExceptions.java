package com.softwareplumbers.feed;

import java.io.IOException;
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
        INVALID_JSON,
        INVALID_ID,
        STREAMING_EXCEPTION
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
        public BaseException(Type type, Exception cause) {
            super(cause);
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
    
    public static class StreamingException extends BaseException {
        public StreamingException(IOException e) {
            super(Type.STREAMING_EXCEPTION, e);
        }
        public StreamingException(String reason) {
            super(Type.STREAMING_EXCEPTION, reason);
        }
        public static Optional<String> getReason(JsonObject object) {
            try {
                return Optional.of(object.getString("reason"));
            } catch (Exception e) {
                return Optional.empty();
            }
        }
        public JsonObjectBuilder buildJson(JsonObjectBuilder bldr) {
            IOException e = (IOException)getCause();
            if (e == null)
                return super.buildJson(bldr).add("reason", getMessage());
            else    
                return super.buildJson(bldr).add("reason", e.getMessage());
        }    
    }
    
    /** Throw if a path is invalid
     * 
     */
    public static class InvalidJson extends BaseException {
        public final Optional<JsonObject> json;
        public final String reason;
        public InvalidJson(String reason, Optional<JsonObject> json) {
            super(Type.INVALID_JSON, reason);
            this.json = json;
            this.reason = reason;
        }
        public InvalidJson(String reason, JsonObject json) {
            this(reason, Optional.of(json));
        }
        public static Optional<JsonObject> getJson(JsonObject object) {
            try {
                return Optional.of(object.getJsonObject("json"));
            } catch (Exception e) {
                return Optional.empty();
            }
        }
        public static Optional<String> getReason(JsonObject object) {
            try {
                return Optional.of(object.getString("reason"));
            } catch (Exception e) {
                return Optional.empty();
            }
        }
        public JsonObjectBuilder buildJson(JsonObjectBuilder bldr) {
            bldr = super.buildJson(bldr).add("reason", reason);
            if (json.isPresent()) bldr = bldr.add("json", json.get());
            return bldr;
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
    
    /** Throw if an id is invalid
     * 
     */
    public static class InvalidId extends BaseException {
        public final FeedPath path;
        public final String id;
        public InvalidId(FeedPath path, String id) {
            super(Type.INVALID_ID, "Invalid Id: " + id + " on path " + path.toString());
            this.path = path;
            this.id = id;
        }
        public static Optional<FeedPath> getPath(JsonObject object) {
            try {
                return Optional.of(FeedPath.valueOf(object.getString("path")));
            } catch (Exception e) {
                return Optional.empty();
            }
        }
        public static Optional<String> getId(JsonObject object) {
            try {
                return Optional.of(object.getString("id"));
            } catch (Exception e) {
                return Optional.empty();
            }
        }
        public JsonObjectBuilder buildJson(JsonObjectBuilder bldr) {
            return super.buildJson(bldr).add("path", path.toString()).add("id", id);
        }
    }
    
    public static BaseException build(JsonObject json) {
        try {
        Type type = BaseException.getType(json).orElseThrow(()->new InvalidJson("missing type", json));
            switch(type) {
                case INVALID_PATH:
                    return new InvalidPath(InvalidPath.getPath(json).orElseThrow(()->new InvalidJson("missing path", json)));
                case INVALID_JSON:
                    return new InvalidJson(InvalidJson.getReason(json).orElseThrow(()->new InvalidJson("missing reason", json)), InvalidJson.getJson(json));
                case INVALID_ID:
                    return new InvalidId(InvalidId.getPath(json).orElseThrow(()->new InvalidJson("missing path", json)), InvalidId.getId(json).orElseThrow(()->new InvalidJson("missing id", json)));
                case STREAMING_EXCEPTION:
                    return new StreamingException(StreamingException.getReason(json).orElseThrow(()->new InvalidJson("missing reason", json)));
                default:
                    throw new InvalidJson("invalid type value", json);
            }
        } catch (InvalidJson e) {
            throw runtime(e);
        }
    }

    /** Remote exceptions contain a message generated by the remote system */
    public static class RemoteException extends BaseRuntimeException {
        
        public RemoteException(JsonObject message) {
            super(build(message));
        }
        
        public RemoteException(BaseException remoteException) {
            super(remoteException);
        }
        
        /** This method allows us to rethrow selected remote exceptions as local exceptions.
         * 
         * @param <T> Type parameter for exception class
         * @param clazz Class object selecting the type of exception we wish to rethrow
         * @throws T if the encapsulated remote exception is an instance of the supplied exception type
         */
        @SuppressWarnings("unchecked")
        public <T extends BaseException> void rethrowAsLocal(Class<T> clazz) throws T {
            if (clazz.isInstance(getCause())) throw (T)getCause();
        }
    }
    
    /** Server errors represent unexpected or unknown errors raised by a remote system */
    public static class ServerError extends BaseException {
        public ServerError(String message) {
            super(null, message);
        }
    }
    
    @FunctionalInterface
    public static interface CheckedConsumer<T> {
        void accept(T t) throws Exception;
    }

    @FunctionalInterface
    public static interface CheckedBiConsumer<T,U> {
        void accept(T t, U u) throws Exception;
    }
    
    public static BaseRuntimeException runtime(BaseException e) {
        return new BaseRuntimeException(e);
    }
    
    public static BaseRuntimeException runtime(IOException e) {
        return runtime(new StreamingException(e));
    }
    
    public static <T> Consumer<T> runtime(final CheckedConsumer<T> consumer) {
        return t->{
            try {
                consumer.accept(t);
            } catch (BaseException e) {
                throw runtime(e);
            } catch (IOException e) {
                throw runtime(e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T,U> BiConsumer<T,U> runtime(final CheckedBiConsumer<T,U> consumer) {
        return (t,u)->{
            try {
                consumer.accept(t,u);
            } catch (BaseException e) {
                throw runtime(e);
            } catch (IOException e) {
                throw runtime(e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }  
    
   
}
