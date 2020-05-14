/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.common.immutablelist.AbstractImmutableList;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

/**
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
    
    public abstract static class Element implements Comparable<Element> {
        
        public enum Type {
            FEEDID,
            FEED,
            MESSAGEID
        }
        
        public final Type type;
        
        public Optional<String> getName() { return Optional.empty(); }
        public Optional<String> getVersion()  { return Optional.empty(); }
        public Optional<String> getId()  { return Optional.empty(); }
        
        public Element(Type type) { this.type = type; }
        @Override
        public boolean equals(Object other) { return other instanceof Element && 0 == compareTo((Element)other); }
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
        public String toString() {
            return escape(name) + (version.isPresent() ? "@" + escape(version.get()) : "");
        }
        
        @Override
        public int hashCode() { return type.hashCode() ^ name.hashCode() ^ version.hashCode(); }  
        
        public static Optional<Feed> valueOf(String item) {
            String[] parts = split(item, "@").toArray(String[]::new);
            if (parts.length > 1)
                return Optional.of(new Feed(unescape(parts[0]), Optional.of(unescape(parts[1]))));
            else
                return Optional.of(new Feed(unescape(parts[0]), Optional.empty()));
        }
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
        public String toString() {
            return "~" + escape(id);
        }
        
        @Override
        public int hashCode() { return type.hashCode() ^ id.hashCode(); }
        
    }
    
    private static class MessageId extends Id {
    
        public MessageId(String id) {
            super(Element.Type.MESSAGEID, id);
        }
    
        public static Optional<Id> valueOf(String item) {
            if (item.startsWith("~") && !item.startsWith("~~")) 
                return Optional.of(new MessageId(unescape(item.substring(1))));
            return Optional.empty();
        }
    }

    private static class FeedId extends Id {
    
        public FeedId(String id) {
            super(Element.Type.FEEDID, id);
        }
    
        public static Optional<Id> valueOf(String item) {
            if (item.startsWith("~") && !item.startsWith("~~")) 
                return Optional.of(new MessageId(unescape(item.substring(1))));
            return Optional.empty();
        }
    }
    
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
    
    public String join(String separator) {
        return join(Element::toString, separator);
    }
    
    private static String escape(String item) {
        return escape(item, "/", DEFAULT_ESCAPE);
    }
    
    private static final String unescape(String escaped) {
        String regexEscape = "\\\\";
        String escape = "\\";
        return escaped
            .replaceAll("(?<!"+ regexEscape +")" + regexEscape, "")
            .replaceAll(regexEscape + regexEscape, escape);
    }
    
 	private static Stream<String> split(String toParse, String separator) {
        String regexEscape = "\\\\";
        String escape = "\\";
        String [] splits = toParse.split("(?<!"+ regexEscape +")" + separator);
        return Stream.of(splits);
	}    
    
    @Override
    public String toString() {
        return join("/");
    }
    
    public FeedPath afterFeedId() {
        if (isEmpty()) return ROOT;
        if (part.type == Element.Type.FEEDID) {
            return ROOT;
        } else {
            return parent.afterFeedId().add(part);
        }
    }
    
    public static FeedPath valueOf(String path) {
        FeedPath result = ROOT;
        Iterable<String> elements = split(path, "/")::iterator;
        for (String pathElement : elements) {
            if (pathElement.length() == 0) continue;  
            Optional<Id> id = result == ROOT ? FeedId.valueOf(pathElement) : MessageId.valueOf(pathElement);
            if (id.isPresent()) {
                result = result.add(id.get());
            } else {
                result = result.add(Feed.valueOf(pathElement)
                    .orElseThrow(()->new RuntimeException("cannot parse: " + pathElement))
                );
            }
        }
        return result;
    }
}
