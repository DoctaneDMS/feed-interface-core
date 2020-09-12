/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import java.util.function.Predicate;

/**
 *
 * @author jonat
 */
public class Filters {
    
    public static final Predicate<Message> NO_ACKS = message->message.getType() != MessageType.ACK;
    public static final Predicate<Message> POSTED_LOCALLY = message->!message.getRemoteInfo().isPresent();
    
}
