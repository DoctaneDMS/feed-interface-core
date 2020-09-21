/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

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
public interface Cluster {
    
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
    
    /** Register a service with this cluster.
     * 
     * Typically a Cluster implementation is a local facade over some network service (e.g. 
     * the Kubernetes API server) and not a service in its own right. This method is called
     * to register some local service as a node on the cluster, effectively advertising the
     * local node on the provided URI.
     * 
     * This method should be called once the given service is ready to start serving requests.
     * Implementers of the cluster interface are responsible for ensuring that a call to 
     * register(...) ultimately results in a call to the init method of the service being 
     * registered (which provides information about the cluster to the service), and that
     * existing services in the cluster are requested to monitor the the new node
     * via a call to the monitor method of each existing service node.
     * 
     * @param service Local service to register
     * @param endpoint endpoint on which the local service handles remote requests
     */
    void register(FeedService service, URI endpoint);
    
    
    void deregister(FeedService service);
    
    /** Utility for creating a local, single-node cluster.
     * 
     * @param service The one and only service in the cluster.
     * @return A trivial cluster object containing a single node.
     */
    public static Cluster local(FeedService service) {
        return new Cluster() {
            @Override
            public Optional<FeedService> getService(UUID id) {
                return Objects.equals(service.getServerId(), id) ? Optional.of(service) : Optional.empty();
            }
            
            @Override
            public Stream<FeedService> getServices(Predicate<FeedService>... filters) {
                return Stream.of(service).filter(Stream.of(filters).reduce(s->true, Predicate::and));
            }

            @Override
            public void register(FeedService service, URI endpoint) {}
            
            @Override
            public void deregister(FeedService service) {}
        };
    }
}
