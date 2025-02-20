/*








 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.InvalidId;
import com.softwareplumbers.feed.FeedExceptions.InvalidPath;
import com.softwareplumbers.feed.FeedExceptions.InvalidState;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;


/** Feed Service interface.
 * 
 * Interface for very simple asynchronous message delivery. Target use case
 * is a RESTful web application server.
 *
 * @author Jonathan Essex.
 */
public interface FeedService extends AutoCloseable {  
    
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
    CompletableFuture<MessageIterator> listen(FeedPath path, Instant after, UUID serverId, long timeoutMillis, Predicate<Message>... filters) throws InvalidPath;
    
    
    /** Watch for messages on a feed posted to this server after the given instant.
     * 
     * Watch is used by remote servers to retrieve messages posted to this server. 
     * 
     * @param watcherServerId ServerId of watching server
     * @param after instant from which to fetch results
     * @return Future which will be completed when at least one matching message arrives.
     */
    CompletableFuture<MessageIterator> watch(UUID watcherServerId, Instant after, long timeoutMillis);
       
    /** Provides cluster or host details to the feed service.
     * 
     * A feed service is registered to a cluster or a host with FeedServiceManager.register(service). Later,
     * the manager object will call back with setManager. close() should ultimately call manager.deregister(service).
     * 
     * It's okay to use a FeedService without ever registering it with a manager. However the manager provides the
     * link to other replicated hosts. When a feed service is registered at 'Cluster' level, the assumption is that
     * the service has some kind of shared persistent state backing it so that we are registering the 'same' service
     * with every host in the cluster. Replication is not needed between services that share state in this way.
     * 
     * If however the service is registered at the host level, it is considered that this service uses host-local
     * storage (or has no persistent storage). Replication needs to be managed between host-registered nodes and
     * other nodes on the cluster.
     * 
     * @param manager 
     */
    void setManager(FeedServiceManager manager);
    
    /** Returns the time at which the feed services was initialized.
     * 
     * A feed service will never have any local messages with a timestamp
     * less than or equal to the init time.
     * 
     * @return 
     */
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
    default MessageIterator search(FeedPath path, UUID serverId, Instant from, Optional<Boolean> relay, Predicate<Message>... filters) throws InvalidPath {
        return search(path, serverId, from, false, Optional.empty(), Optional.empty(), Optional.of(true), filters);
    }
    
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
    default MessageIterator search(FeedPath path, UUID serverId, Instant from, Predicate<Message>... filters) throws InvalidPath {
        return search(path, serverId, from, false, Optional.empty(), Optional.empty(), Optional.of(true), filters);
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
    MessageIterator search(FeedPath path, UUID serverId, Instant from, boolean fromInclusive, Optional<Instant> to, Optional<Boolean> toInclusive, Optional<Boolean> relay, Predicate<Message>... filters) throws InvalidPath;

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
    default MessageIterator search(FeedPath path, UUID serverId, Instant from, boolean fromInclusive, Optional<Instant> to, Optional<Boolean> toInclusive,  Predicate<Message>... filters) throws InvalidPath {
        return search(path, serverId, from, fromInclusive, to, toInclusive, Optional.of(true), filters);
    }
    
    /** Sent a message to a feed.
     * 
     * The service will ignore any timestamp, serverId, or message id specified in the message.
     * 
     * The returned message is an ACK containing (at least) the generated  message id, timestamp,
     * and serverId (which will be the same as that returned by this.getServerId).
     * 
     * @param path Path of feed to send message to.
     * @param message
     * @return An ack message with updated timestamp, name, and id
     * @throws InvalidPath 
     * @throws InvalidState 
     */
    Message post(FeedPath path, Message message) throws InvalidPath, InvalidState;
    
    Message replicate(Message message) throws InvalidState;
    
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
    
    /** Get the Cluster to which this service belongs, if any.
     * 
     * The Cluster object can be used to access other servers in the same cluster
     * as 'this' one.
     * 
     * @return A Cluster object.
     */    
    Optional<Cluster> getCluster();
    
    public Feed getFeed(FeedPath path) throws InvalidPath;

    public Stream<Feed> getChildren(FeedPath path) throws InvalidPath;
    
    public Stream<Feed> getFeeds();
    
    /** Get the most recent message available here.
     * 
     * Typically used to bootstrap listening to a feed when we don't care about 
     * old messages. This should return the timestamp of the most message available 
     * locally (it won't go out to the cluster to look for more recent messages). 
     * 
     * If Optional.empty is returned, this implies there is no message available 
     * locally on this feed. In this case, the user should use the value returned
     * by getInitTime.
     * 
     * @param path
     * @return The
     * @throws com.softwareplumbers.feed.FeedExceptions.InvalidPath 
     */
    public Optional<Instant> getLastTimestamp(FeedPath path) throws InvalidPath;
    
    public void dumpState(PrintWriter out);
    
    public static Instant getInitTime(JsonObject json) {
        return Instant.parse(json.getString("initTime"));
    }
    
    public static UUID getServerId(JsonObject json) {
        return UUID.fromString(json.getString("serviceId"));
    }
    
    default JsonObject toJson() {
        JsonObjectBuilder jsonResult = Json.createObjectBuilder();
        jsonResult.add("initTime", getInitTime().toString());
        jsonResult.add("serviceId", getServerId().toString());
        return jsonResult.build();
    }
}
