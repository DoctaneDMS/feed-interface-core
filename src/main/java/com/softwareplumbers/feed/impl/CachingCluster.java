/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.Cluster;
import com.softwareplumbers.feed.FeedService;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/** Implementation of the Cluster which caches some underlying service.
 *
 * @author jonathan essex
 */
public abstract class CachingCluster implements Cluster {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(CachingCluster.class);
    public static final long CACHE_TTL_MILLISECOND = 10000L;
    private final Map<UUID,RegistryElement> cache = new ConcurrentHashMap<>();
    private Instant expiry = Instant.EPOCH;
    private final Object expiryUpdateLock = new Object();
    private final Set<Replicator> remotes = new HashSet<>();
    private final ExecutorService executor;
    
    public CachingCluster(ExecutorService executor) {
        this.executor = executor;
    }
    
    public static class RegistryElement {
        public final UUID serviceId;
        public final URI uri;
        public final Instant expiry;
        public FeedService service;
        
        public RegistryElement(UUID serviceId, URI uri, FeedService service) { 
            this.serviceId = serviceId; 
            this.uri = uri; 
            this.expiry = Instant.now().plusMillis(CACHE_TTL_MILLISECOND); 
            this.service = service;
        }
        
        public FeedService getFeedService(Function<URI,FeedService> resolver) {
            if (service == null) {
                service = resolver.apply(uri);
            }
            return service;
        }
        
        public static RegistryElement fromJson(JsonObject json) {
            return new RegistryElement(
                UUID.fromString(json.getString("serviceId")),
                URI.create(json.getString("uri")),
                null
            );
        }
        
        public JsonObject toJson() {
            JsonObjectBuilder builder = Json.createObjectBuilder();
            return builder.add("serviceId", serviceId.toString()).add("uri", uri.toString()).build();
        }
        
        public static RegistryElement merge(RegistryElement existing, RegistryElement update) {
            if (existing == null) return update;
            if (update == null) return null;
            update.service = existing.service;
            return update;
        }
        
        public String toString() {
            return String.format("RegistryEntry[expires %s,  %s]", expiry, toJson());
        }
    }
    
    @Override
    public Optional<FeedService> getService(UUID id) {
        LOG.entry(id);
        return LOG.exit(
            Optional.ofNullable(cache.compute(id, (key,value)->
                Instant.now().isAfter(value.expiry) 
                    ? RegistryElement.merge(value, fetch(id))
                    : value
            ))
                .map(entry->entry.getFeedService(this::getRemote))
        );
    }    
    
    @Override
    public Stream<FeedService> getServices(Predicate<FeedService>... filters) {
        LOG.entry((Object[])filters);
        boolean updateCache = false;
        
        // Prevent multiple updates running concurrently. However, we are good
        // with processess seeing old data while the update is runnig
        synchronized(expiryUpdateLock) {
            if (Instant.now().isAfter(expiry)) {
                expiry = Instant.now().plusMillis(CACHE_TTL_MILLISECOND);
                updateCache = true;
            }
        }
        
        if (updateCache) {
            // This will renew all the elements in the cache
                fetchAll()
                .forEach(element -> cache.merge(element.serviceId, element, RegistryElement::merge));
            // This will remove any expired elements from the cache
            cache.entrySet().removeIf(entry->Instant.now().isAfter(entry.getValue().expiry));
        }
        
        return LOG.exit(
            cache.values().stream()
                .map(entry->entry.getFeedService(this::getRemote))
                .filter(Stream.of(filters).reduce(i->true, Predicate::and))
        );
    }
    
    public void replicate(FeedService to, FeedService from) {
        LOG.entry(to, from);
        LOG.debug("Service {} will replicate data from {}", to, from);
        Replicator replicator = new Replicator(executor, to, from);
        synchronized(this) {
            if (remotes.add(replicator)) {
                replicator.startMonitor();
            }
        }
        LOG.exit();
    }    
    
    @Override
    public void register(FeedService service, URI endpoint) {
        LOG.entry(service, endpoint);
        UUID serverId = service.getServerId();
        synchronized(this) {
            try (AutoCloseable lock = lock(serverId)) {
                service.initialize(this);
                RegistryElement entry = new RegistryElement(serverId, endpoint, service);
                cache.put(serverId, entry);
                save(entry);
                getServices(Cluster.Filters.idIsNot(serverId)).forEach(other->{
                     replicate(service, other);
                     replicate(other, service);
                });
            } catch (Exception ex) {
                LOG.error("Error registering feed service in cluster", ex);
                throw LOG.throwing(new RuntimeException(ex));
            }
        }
        LOG.exit();
    }
    
    @Override
    public void deregister(FeedService service) {
        LOG.entry(service);
        UUID serverId = service.getServerId();
        synchronized(this) {
            remotes.removeIf(replicator->replicator.getFrom().getServerId().equals(serverId) || replicator.getTo().getServerId().equals(serverId));
            cache.remove(serverId);
            remove(serverId);            
        }
        LOG.exit();
    }
    
    public void dumpState(PrintWriter out) throws IOException {
        remotes.forEach(remote->remote.dumpState(out));
    }    
    
    public abstract FeedService getRemote(URI endpoint);
    public abstract RegistryElement fetch(UUID id);
    public abstract Stream<RegistryElement> fetchAll();
    public abstract void remove(UUID id);
    public abstract AutoCloseable lock(UUID serverId);
    public abstract void save(RegistryElement toSave);    
}
