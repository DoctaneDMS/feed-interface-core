/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import java.io.PrintWriter;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;

/** Interface used to interact with a cluster of nodes implementing a feed service.
 *
 * @author Jonathan Essex
 */
public interface Cluster extends AutoCloseable {
    
    public static class Filters {
        
        public static class IdIsNot implements Predicate<FeedService> {
            public final UUID serviceId;
            public IdIsNot(UUID serviceId) { this.serviceId = serviceId; }

            @Override
            public boolean test(FeedService t) {
                return !t.getServerId().equals(serviceId);
            }
            @Override
            public String toString() {
                return "Filter[Id!=" + serviceId + "]";
            }
        }
        
        public static Predicate<FeedService> idIsNot(UUID id) { return new IdIsNot(id); }
        
    }
    
    /** Get the service endpoint for the cluster node identified by the supplied id
     * 
     * @param id
     * @return A FeedService object connected directly to the requested endpoint.
     */
    default Optional<FeedService> getService(UUID id) {
        return getServices(service->Objects.equals(service.getServerId(), id)).findAny();
    }
    
    /** List the active nodes in this cluster.
     * 
     * @param filters Filter which service objects we want to see.
     * @return A stream of FeedService objects, any of which can be used to access feeds in this cluster.
     */
    Stream<FeedService> getServices(Predicate<FeedService>... filters);
    
    /** Register a local service with this cluster.
     * 
     * Typically a Cluster implementation is a local facade over some network service (e.g. 
     * the Kubernetes API server) and not a service in its own right. This method is called
     * to register some local service as a node on the cluster, effectively advertising the
     * local node on the provided URI. (The user is responsible for ensuring the service
     * actually responds to the given URI)
     * 
     * This method should be called once the given service is ready to start serving requests.
     * Implementers of the cluster interface are responsible for ensuring that a call to 
     * register(...) ultimately results in a call to the init method of the service being 
     * registered (which provides information about the cluster to the service), and that
     * replicate will be called as appropriate to set up replication between the new node
     * and existing nodes in the cluster.
     * 
     * @param service Local service to register
     * @param endpoint endpoint on which the local service handles remote requests
     */
    void register(FeedService service, URI endpoint);
    
    void register(UUID serverId, URI endpoint);
    
    void deregister(FeedService service);
    
    /** Set up replication in one direction between two feed services in the cluster 
     * 
     * @param to Feed service receiving messages
     * @param from Feed service supplying messages
     */
    void replicate(UUID to, UUID from);
    
    void dumpState(PrintWriter out);
}
