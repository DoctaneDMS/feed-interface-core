/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import java.util.UUID;
import java.util.function.Predicate;

/**
 *
 * @author jonat
 */
public class Filters {
    
    public static final Predicate<Message> NO_ACKS = message->message.getType() != MessageType.ACK;
    public static final Predicate<Message> IS_ACK = message->message.getType() == MessageType.ACK;
    public static final Predicate<Message> POSTED_LOCALLY = message->!message.getRemoteInfo().isPresent();
    
    public static class FromRemote implements Predicate<Message>  {
        public final UUID remote;

        public FromRemote(UUID remote) {
            this.remote = remote;
        }
        
        @Override
        public boolean test(Message message) {
            return message.getRemoteInfo().isPresent() && message.getRemoteInfo().get().serverId.equals(remote);
        }
    }
    
    public static Predicate<Message> fromRemote(UUID remote) { return new FromRemote(remote); }
    
}
