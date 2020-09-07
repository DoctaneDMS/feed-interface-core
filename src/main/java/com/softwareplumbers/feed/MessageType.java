/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** A message type.
 *
 * @author Jonathan Essex
 */
public class MessageType implements Comparable<MessageType> {
    
    private static final Map<String,MessageType> types = new ConcurrentHashMap<>();
    private final String name;
    
    public static final MessageType ACK = valueOf("ACK");
    public static final MessageType NONE = valueOf("NONE");
    
    private MessageType(String name) {
        this.name = name;
    }
    
    public static MessageType valueOf(String name) {
        if (name == null) {
            return NONE;
        } else {
            return types.computeIfAbsent(name, k -> new MessageType(k));
        }
    }
    
    @Override
    public String toString() {
        return name;
    }

    @Override
    public int compareTo(MessageType other) {
        return name.compareTo(other.name);
    }
}
