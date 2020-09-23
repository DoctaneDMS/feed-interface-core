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
import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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
    private final Resolver<FeedService> feedServiceResolver;
    private final Resolver<Cluster> clusterResolver;
    private Map<URI,JsonObject> credentials = new ConcurrentHashMap<>();
    private JsonObject defaultCredential = JsonObject.EMPTY_JSON_OBJECT;
    
    
    public CachingCluster(ExecutorService executor, Resolver<FeedService> feedServiceResolver, Resolver<Cluster> clusterResolver) {
        this.executor = executor;
        this.feedServiceResolver = feedServiceResolver;
        this.clusterResolver = clusterResolver;
    }
    
    public static class RegistryElement {
        public final UUID serviceId;
        public final URI uri;
        public final Instant expiry;
        public boolean local;
        public FeedService service;
        
        public RegistryElement(UUID serviceId, URI uri, FeedService service) { 
            this.serviceId = serviceId; 
            this.uri = uri; 
            this.expiry = Instant.now().plusMillis(CACHE_TTL_MILLISECOND); 
            this.service = service;
            this.local = true;
        }
        
        public RegistryElement(UUID serviceId, URI uri) {
            this.serviceId = serviceId; 
            this.uri = uri; 
            this.expiry = Instant.now().plusMillis(CACHE_TTL_MILLISECOND); 
            this.service = null;
            this.local = false;
        }
        
        public FeedService getFeedService(Cluster cluster, JsonObject credentials, Resolver<FeedService> resolver) {
            if (service == null) {
                service = resolver.resolve(uri, credentials)
                    .orElseThrow(()->new RuntimeException("Can't resovle feed service"));
                service.setCluster(cluster);
            }
            return service;
        }
        
        public static RegistryElement fromJson(JsonObject json) {
            return new RegistryElement(
                UUID.fromString(json.getString("serviceId")),
                URI.create(json.getString("uri"))
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
            update.local = existing.local;
            return update;
        }
        
        @Override
        public String toString() {
            return String.format("RegistryEntry[expires %s,  %s]", expiry, toJson());
        }
        
        public boolean isLocal() {
            return local;
        }
    }
    
    public Optional<RegistryElement> getNodeInfo(UUID id) {
        LOG.entry(id);
        return LOG.exit(
            Optional.ofNullable(cache.compute(id, (key,value)->
                value == null || Instant.now().isAfter(value.expiry) 
                    ? RegistryElement.merge(value, fetch(id))
                    : value
            ))
        );
    }
    
    public JsonObject getCredential(URI service) {
        JsonObject credential = credentials.get(service);
        return credential == null ? defaultCredential : credential;
        
    }
    
    @Override
    public Optional<FeedService> getService(UUID id) {
        LOG.entry(id);
        return LOG.exit(getNodeInfo(id).map(entry->entry.getFeedService(this, getCredential(entry.uri), feedServiceResolver)));
    }    
    
    Stream<RegistryElement> getNodeInfo() {
        boolean updateCache = false;
        
        Instant now = Instant.now();
        // Prevent multiple updates running concurrently. However, we are good
        // with processess seeing old data while the update is runnig
        synchronized(expiryUpdateLock) {
            if (now.isAfter(expiry)) {
                expiry = Instant.now().plusMillis(CACHE_TTL_MILLISECOND);
                updateCache = true;
            }
        }
        
        if (updateCache) {
            // This will renew all the elements in the cache
                fetchAll()
                .forEach(element -> cache.merge(element.serviceId, element, RegistryElement::merge));
            // This will remove any expired elements from the cache
            cache.entrySet().removeIf(entry->now.isAfter(entry.getValue().expiry));
        }

        return cache.values().stream();
    }
    
    @Override
    public Stream<FeedService> getServices(Predicate<FeedService>... filters) {
        LOG.entry((Object[])filters);
        
        return LOG.exit(
            getNodeInfo()
                .map(entry->entry.getFeedService(this, getCredential(entry.uri), feedServiceResolver))
                .filter(Stream.of(filters).reduce(i->true, Predicate::and))
        );
    }
    
    private void replicate(RegistryElement to, RegistryElement from) {
        LOG.entry(to, from);
        LOG.debug("Service {} will replicate data from {}", to, from);
        if (to.isLocal()) {
            Replicator replicator = new Replicator(executor, to.getFeedService(this, getCredential(to.uri), feedServiceResolver), from.getFeedService(this, getCredential(from.uri), feedServiceResolver));
            synchronized(this) {
                if (remotes.add(replicator)) {
                    replicator.startMonitor();
                }
            }
        } else {
            clusterResolver.resolve(to.uri, getCredential(to.uri))
                .orElse(this) // in the case where URI is a distributed node
                .replicate(to.serviceId, from.serviceId);
        }
        LOG.exit();
    }    
    
    @Override
    public void replicate(UUID from, UUID to) {
        replicate(
            getNodeInfo(from).orElseThrow(()->new RuntimeException("Invalid node " + from)), 
            getNodeInfo(to).orElseThrow(()->new RuntimeException("Invalid node " + to))
        );
    }
    
    @Override
    public void register(FeedService service, URI endpoint) {
        LOG.entry(service, endpoint);
        UUID serverId = service.getServerId();
        synchronized(this) {
            try (AutoCloseable lock = lock(serverId)) {
                service.setCluster(this);
                RegistryElement entry = new RegistryElement(serverId, endpoint, service);
                cache.put(serverId, entry);
                save(entry);
                getNodeInfo().filter(node->!node.serviceId.equals(serverId)).forEach(other->{
                     replicate(entry, other);
                     replicate(other, entry);
                });
            } catch (Exception ex) {
                LOG.error("Error registering feed service in cluster", ex);
                throw LOG.throwing(new RuntimeException(ex));
            }
        }
        LOG.exit();
    }
    
    @Override
    public void register(UUID serverId, URI endpoint) {
        LOG.entry(endpoint);
        synchronized(this) {
            try (AutoCloseable lock = lock(serverId)) {
                //service.initialize(this);
                RegistryElement entry = new RegistryElement(serverId, endpoint);
                cache.put(serverId, entry);
                save(entry);
                getNodeInfo().filter(node->!node.serviceId.equals(serverId)).forEach(other->{
                     replicate(entry, other);
                     replicate(other, entry);
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
    
    public void setCredential(URI remote, JsonObject credential) {
        credentials.put(remote, credential);
    }

    public void setCredential(URI remote, String username, String password) {
        JsonObjectBuilder credential = Json.createObjectBuilder();
        credential.add("username", username);
        credential.add("password", password);
        setCredential(remote, credential.build());
    }
    
    public void setCredentials(Map<URI, JsonObject> credentials) {
        this.credentials.clear();
        this.credentials.putAll(credentials);
    }
   
    public void dumpState(PrintWriter out) {
        remotes.forEach(remote->remote.dumpState(out));
    }    
    
    public abstract RegistryElement fetch(UUID id);
    public abstract Stream<RegistryElement> fetchAll();
    public abstract void remove(UUID id);
    public abstract AutoCloseable lock(UUID serverId);
    public abstract void save(RegistryElement toSave);    
}
