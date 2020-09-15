/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidId;
import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Stream;


/** Feed Service interface.
 * 
 * Interface for very simple asynchronous message delivery. Target use case
 * is a RESTful web application server.
 *
 * @author Jonathan Essex.
 */
public interface FeedService {  
    
    /** Listen for all messages on a feed after the given instant.
     * 
     * Listen should return all messages with a timestamp greater than 
     * the given instant. 
     * 
     * @param path Path to feed (must not include a message id)
     * @param after Instant after which we listen for new messages
     * @param serverId Server id indicates which server's timestamps to use when comparing with 'from'.
     * @param filters Predicates which will filter the returned message stream.
     * @return Future which will be completed when at least one matching message arrives.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    CompletableFuture<MessageIterator> listen(FeedPath path, Instant after, UUID serverId, Predicate<Message>... filters) throws InvalidPath;
    
    
    /** Watch for messages on a feed posted to this server after the given instant.
     * 
     * Watch is used by remote servers to retrieve messages posted to this server. If we are not already 
     * watching watcherServerId, we will watch it back.
     * 
     * @param watcherServerId ServerId of watching server
     * @param after instant from which to fetch results
     * @return Future which will be completed when at least one matching message arrives.
     */
    CompletableFuture<MessageIterator> watch(UUID watcherServerId, Instant after);
       
    /** Provides cluster details to the feed service.
     * 
     * A feed service is registered to a cluster with Cluster.register(service). Later,
     * the cluster will call back FeedService.initialize(cluster); Cluster guarantees
     * that initialize will be called at most once and that the service is fully registered
     * by the cluster before initialize is called.
     * 
     * @param cluster 
     */
    void initialize(Cluster cluster);
    
    /** Monitor a remote feed service for new messages.
     * 
     * Typically called automatically during feed initialization.
     * 
     * @param service 
     */
    void monitor(FeedService service);
    
    Instant getInitTime();
    
    /** Get all messages sharing the given message Id.
     * 
     * Typically a message and its related ACKs will share the same message Id. Generally
     * we expect a message to be acknowledged by each server in the cluster, so this method
     * can return up to n+1 messages in a cluster with n servers.
     * 
     * @param messageId The message Id we are searching for
     * @param filters Optional predicates to filter the set of returned results 
     * @return All messages which share the given id.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidId 
     */
    public MessageIterator search(FeedPath messageId, Predicate<Message>... filters) throws InvalidPath, InvalidId;

    /** Get messages synchronously.
     * 
     * Search will return all messages with a timestamp after the given instant. The serverId
     * flag indicates which server the 'from' timestamp came from (since different servers
     * may  have a different view of time). If the relay option is 'true', then the search will
     * be relayed to other servers in the cluster if all requested messages are not available 
     * locally.
     * 
     * @param path Path to feed (must not include a message id)
     * @param from Instant after which we search for new messages
     * @param serverId Server id indicates which server's timestamps to use when comparing with 'from'.
     * @param relay If true, relay this search to other servers in the cluster
     * @param filters Optional predicates to filter the set of returned results 
     * @return A MessageIterator WHICH MUST BE CLOSED.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    MessageIterator search(FeedPath path, Instant from, UUID serverId, boolean relay, Predicate<Message>... filters) throws InvalidPath;
    
    /** Get messages synchronously.
     * 
     * Convenience method equivalent to search(path, from, serverId, true, filters)
     * 
     * @param path Path to feed (must not include a message id)
     * @param from Instant after which we search for new messages
     * @param serverId Server id indicates which server's timestamps to use when comparing with 'from'.
     * @param filters Optional predicates to filter the set of returned results 
     * @return A MessageIterator WHICH MUST BE CLOSED.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    default MessageIterator search(FeedPath path, Instant from, UUID serverId, Predicate<Message>... filters) throws InvalidPath {
        return search(path, from, serverId, true, filters);
    }
    
    /** Get messages synchronously.
     * 
     * Search will return all messages with a timestamp between the given instants. The serverId
     * flag indicates which server the timestamps came from (since different servers
     * may  have a different view of time). If the relay option is 'true', then the search will
     * be relayed to other servers in the cluster if all requested messages are not available 
     * locally.
     * 
     * @param path Path to feed (must not include a message id)
     * @param from Instant from which we search for new messages
     * @param fromInclusive indicates that the returned values should include timestamps equal to from
     * @param to Instant to which we search for new messages
     * @param toInclusive indicates that the returned values should include timestamps equal to from
     * @param serverId Server id indicates which server's timestamps to use when comparing with 'from'.
     * @param relay If true, relay this search to other servers in the cluster
     * @param filters Optional predicates to filter the set of returned results 
     * @return A MessageIterator WHICH MUST BE CLOSED.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    MessageIterator search(FeedPath path, Instant from, boolean fromInclusive, Instant to, boolean toInclusive, UUID serverId, boolean relay, Predicate<Message>... filters) throws InvalidPath;

    /** Get messages synchronously.
     * 
     * Convenience method equivalent to search(path, from, fromInclusive, to, toInclusive, serverId, true, filters)
     * 
     * @param path Path to feed (must not include a message id)
     * @param from Instant after which we search for new messages
     * @param fromInclusive indicates that the returned values should include timestamps equal to from
     * @param to Instant to which we search for new messages
     * @param toInclusive indicates that the returned values should include timestamps equal to from
     * @param serverId Server id indicates which server's timestamps to use when comparing with 'from'.
     * @param filters Optional predicates to filter the set of returned results 
     * @return A MessageIterator WHICH MUST BE CLOSED.
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    default MessageIterator search(FeedPath path, Instant from, boolean fromInclusive, Instant to, boolean toInclusive, UUID serverId, Predicate<Message>... filters) throws InvalidPath {
        return search(path, from, fromInclusive, to, toInclusive, serverId, true, filters);
    }
    
    /** Sent a message to a feed.
     * 
     * The service will ignore any timestamp, serverId, or message id specified in the 
     * message. The returned message is an ACK containing (at least) the generated 
     * message id, timestamp, and serverId (which will be the same as that returned by
     * this.getServerId).
     * 
     * @param path Path of feed to send message to.
     * @param message
     * @return An ack message with updated timestamp, name, and id
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    Message post(FeedPath path, Message message) throws InvalidPath;
    
    Message replicate(Message message);
    
    /** Get the Id of this server instance.
     * 
     * Different servers may have a different view of the sequence in which messages arrive. To
     * avoid potentially dropping messages when a restart or a fail-over occurs, clients need
     * to know which server they are listening to and supply that information when they try
     * to re-connect to a new server. (see the sync method above).
     * 
     * @return 
     */
    UUID getServerId();
    
    /** Get the Cluster to which this service belongs.
     * 
     * The Cluster object can be used to access other servers in the same cluster
     * as 'this' one.
     * 
     * @return A Cluster object.
     */
    Cluster getCluster();
    
    public Feed getFeed(FeedPath path) throws InvalidPath;
    
    public Stream<Feed> getFeeds();
}
