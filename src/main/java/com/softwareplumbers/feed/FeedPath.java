/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.common.immutablelist.AbstractImmutableList;
import com.softwareplumbers.common.abstractpattern.parsers.Tokenizer;
import com.softwareplumbers.common.abstractpattern.parsers.Token;
import com.softwareplumbers.feed.FeedExceptions.InvalidPathSyntax;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

/** Defines the path to a feed or message.
 * 
 * Paths are of the form feed/feed/~messageid where
 * the messageid component is optional and as many feed
 * names as desired can be added to the hierarchy.
 * 
 * Feed Paths are comparable, with earlier elements being compared first. Paths
 * starting with similar subsequences are ordered such that the shorter paths appear
 * first.
 * 
 * A feed Id may not be valid across servers.
 *
 * @author Jonathan Essex
 */
public class FeedPath extends AbstractImmutableList<FeedPath.Element, FeedPath> {
    
    private static final Comparator<Optional<String>> COMPARE_VERSIONS = (a,b) -> {
        if (a.isPresent() && b.isPresent()) return a.get().compareTo(b.get());
        if (a.isPresent()) return -1;
        if (b.isPresent()) return 1;
        return 0;
    };
    
    private static final String[] FEED_PATH_OPERATORS = {"/", "@", "~"};
    
    private static final char DEFAULT_ESCAPE = '\\';        
    
    protected static String escape(final String toEscape, final int escape) {
        final StringBuilder builder = new StringBuilder();
        final String escapeString = Character.toString((char)escape);
        final String[] operators = Stream.concat(Stream.of(FEED_PATH_OPERATORS), Stream.of(escapeString)).toArray(String[]::new);
        Tokenizer tokenizer = new Tokenizer(toEscape, operators);
       
        while (tokenizer.hasNext()) { 
            Token token = tokenizer.next();
            if (token.type == Token.Type.OPERATOR) builder.append(escapeString);
            builder.append(token.data);
        }
        
        return builder.toString();
    }            
                
    /** Represents an element in a feed path.
     * 
     */
    public abstract static class Element implements Comparable<Element> {
        
        /** Represents the different types of an element in the feed path.
         * 
         */
        public enum Type {
            FEED,
            MESSAGEID
        }
        
        /** The type of this particular element */
        public final Type type;
        
        /** Get the name of this element, if it has one */
        public Optional<String> getName() { return Optional.empty(); }
        /** Get the version of this element, if it has one */
        public Optional<String> getVersion()  { return Optional.empty(); }
        /** Get the id of this element, if it has one */
        public Optional<String> getId()  { return Optional.empty(); }
        
        public Element(Type type) { this.type = type; }
        @Override
        public boolean equals(Object other) { return other instanceof Element && 0 == compareTo((Element)other); }
        
        public abstract String toString(char escape);
        
        public String toString() {
            return toString(DEFAULT_ESCAPE);
        }
    }
    
    private static class Feed extends Element {
        public final String name;
        public final Optional<String> version;
        
        public Feed(String name, Optional<String> version) {
            super(Type.FEED);
            this.name = name;
            this.version = version;
        }
        
        @Override
        public Optional<String> getName() { return Optional.of(name); }
        @Override
        public Optional<String> getVersion() { return version; }
        
        @Override
        public int compareTo(Element other) {
            int result = type.compareTo(other.type);
            if (result == 0) {
                Feed otherFeed = (Feed)other;
                result = name.compareTo(otherFeed.name);
                if (result == 0) {
                    result = COMPARE_VERSIONS.compare(version, otherFeed.version);
                }
            }
            return result;
        }
        
        @Override
        public String toString(char escape) {
            return escape(name, escape) + (version.isPresent() ? "@" + escape(version.get(), escape) : "");
        }
        
        @Override
        public int hashCode() { return type.hashCode() ^ name.hashCode() ^ version.hashCode(); }  
        

    }
    
    private static class Id extends Element {
        public final String id;
        
        public Id(Type type, String id) {
            super(Type.MESSAGEID);
            this.id = id;
        }
        
        @Override
        public Optional<String> getId() { return Optional.of(id); }
        
        @Override
        public int compareTo(Element other) {
            int result = type.compareTo(other.type);
            if (result == 0) {
                Id otherId = (Id)other;
                result = id.compareTo(otherId.id);
            }
            return result;
        }        
        
        @Override
        public String toString(char escape) {
            return "~" + escape(id, escape);
        }
        
        @Override
        public int hashCode() { return type.hashCode() ^ id.hashCode(); }
        
    }
    
    private static class MessageId extends Id {
    
        public MessageId(String id) {
            super(Element.Type.MESSAGEID, id);
        }
    }
  
    /** An empty feed path */
    public static final FeedPath ROOT = new FeedPath(null, null) {
        @Override
        public boolean isEmpty() { return true; }
    };
    
    private FeedPath(FeedPath parent, Element part) {
        super(parent, part);
    }
    
    @Override
    public FeedPath getEmpty() {
        return ROOT;
    }

    @Override
    public FeedPath add(Element t) {
        return new FeedPath(this, t);
    }
    
    public FeedPath add(String name) {
        return add(new Feed(name, Optional.empty()));
    }
    
    public FeedPath addId(String id) {
        return add(new MessageId(id));
    }
    
    public FeedPath setVersion(String version) {
        if (part.type == Element.Type.FEED) {
            return parent.add(new Feed(part.getName().get(), Optional.of(version)));
        } else {
            throw new RuntimeException("Can't set version for an id");
        }
    }
    
    public String join(String separator, char escape) {
        return isEmpty() ? "" : parent.toString(escape) + separator + part.toString(escape);
    }
 
    
    @Override
    public String toString() {
        return toString(DEFAULT_ESCAPE);
    }
    
    public String toString(char escape) {
        return join("/", escape);
    }
    

    private FeedPath beforeMessageIdOrEmpty() {
        if (isEmpty()) return ROOT;
        if (part.type == Element.Type.MESSAGEID) {
            return parent;
        } else {
            return parent.beforeMessageIdOrEmpty();
        }        
    }

    public FeedPath beforeMessageId() {
        FeedPath fp = beforeMessageIdOrEmpty();
        return fp.isEmpty() ? this : fp; 
    }
    
    private static String parseVersion(Tokenizer tokenizer) throws InvalidPathSyntax {
        Token token = tokenizer.current();
        if (token.type == Token.Type.CHAR_SEQUENCE) {
            String version = tokenizer.current().data.toString();
            tokenizer.next();
            return version;
        } else {
            throw new InvalidPathSyntax("Unexpected character " + token.data, tokenizer.getStatus());            
        }        
    }
    
    private static Feed parseName(Tokenizer tokenizer) throws InvalidPathSyntax {
        Token token = tokenizer.current();
        if (token.type == Token.Type.CHAR_SEQUENCE) {
            String name = tokenizer.current().data.toString();
            Optional<String> version = Optional.empty();
            tokenizer.next();
            if (tokenizer.hasNext()) {
                token = tokenizer.current();        
                if (token.type == Token.Type.OPERATOR) {
                    if ("@".contentEquals(token.data)) {
                        tokenizer.next();
                        version = Optional.of(parseVersion(tokenizer));
                    }
                }
            }
            return new Feed(name, version);
        } else {
            throw new InvalidPathSyntax("Unexpected character " + token.data, tokenizer.getStatus());            
        }
    }    
    
    private static Element parseId(Tokenizer tokenizer) throws InvalidPathSyntax {
        Token token = tokenizer.current();
        if (token.type == Token.Type.OPERATOR) {
            throw new InvalidPathSyntax("Unexpected character " + token.data, tokenizer.getStatus());
        } else {
            CharSequence id = token.data;
            tokenizer.next();
            return new MessageId(id.toString());
        }
    }    
    
    private static Element parsePathElement(Tokenizer tokenizer) throws InvalidPathSyntax {
        Token token = tokenizer.current();
        if (token.type == Token.Type.OPERATOR) {
            if ("~".contentEquals(token.data)) {
                tokenizer.next();
                return parseId(tokenizer);
            } else {
                throw new InvalidPathSyntax("Unexpected symbol " + token.data, tokenizer.getStatus());
            }
        } else {
            return parseName(tokenizer);
        }
    }
        
    private static FeedPath parsePath(Tokenizer tokenizer) throws InvalidPathSyntax {
        FeedPath result = FeedPath.ROOT;
        while (tokenizer.hasNext()) {
            Token token = tokenizer.current();
            if (token.type == Token.Type.OPERATOR) {
                if ("/".contentEquals(token.data)) {
                    tokenizer.next();                    
                    continue;
                } else if ("@".contentEquals(token.data)) {
                    throw new InvalidPathSyntax("Unexpected symbol `@`", tokenizer.getStatus());
                }
                // other operators must be part of the path element
            }
            result = result.add(parsePathElement(tokenizer));
        } 
        return result;
    }
    
    public static FeedPath valueOf(String path, char escape) throws InvalidPathSyntax {
        FeedPath result = ROOT;
        
        Tokenizer tokenizer = new Tokenizer(path, escape, FEED_PATH_OPERATORS);
        
        return parsePath(tokenizer);
    }
        
    
    /** Convert a string into a feed path.
     * 
     * Paths are of the form ~feedid/feed/feed/~messageid where
     * the feedid and messageid components are optional and as many feed
     * names as desired can be added to the hierarchy.     * 
     * @param path String to convert into a feed path
     * @return The feed path.
     */
    public static FeedPath valueOf(String path) throws InvalidPathSyntax {
        return valueOf(path, DEFAULT_ESCAPE);
    }
}
