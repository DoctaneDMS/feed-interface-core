/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.Cluster;
import com.softwareplumbers.feed.FeedService;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.json.JsonValue;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/** Implementation of the Cluster which caches some underlying service.
 *
 * @author jonathan essex
 */
public abstract class AbstractCluster implements Cluster {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(AbstractCluster.class);

    private final ExecutorService executor;    
    private Map<UUID,FeedService> clusterServices = new ConcurrentHashMap<>();
    private final Supplier<StatusMap> statusSupplier;
    
    private class LocalHostImpl implements LocalHost {

        private final Set<Replicator> replicators = new HashSet<>();
        private final Map<UUID,FeedService> localServices = new ConcurrentHashMap<>();

        private void replicateLocally(FeedService from, FeedService to) {
            Replicator replicator = new Replicator(executor, to, from);
            synchronized(this) {
                if (replicators.add(replicator)) {
                    replicator.startMonitor();
                }
            }        
        }
        
        private Optional<FeedService> getLocalService(UUID id) {
            return Optional.ofNullable(
                Optional.ofNullable(localServices.get(id)).orElseGet(()->clusterServices.get(id))
            );
        }

        @Override
        public void replicate(UUID from, UUID to) {
            Optional<FeedService> localService = getLocalService(to);
            if (localService.isPresent()) {
                replicateLocally(
                    getService(from).orElseThrow(()->new RuntimeException("Invalid service id" + from)), 
                    localService.get()
                );
            } else {
                getHost(to).orElseThrow(()->new RuntimeException("no host for " + to)).replicate(from, to);
            }
        }

        @Override
        public void closeReplication(UUID service) {
            LOG.entry(service);
            replicators.stream()
                .filter(replicator->replicator.touches(service)).forEach(Replicator::close);
            replicators
                .removeIf(replicator->replicator.touches(service));        
            LOG.exit();
        }

        @Override
        public Stream<FeedService> getLocalServices() {
            return Stream.concat(localServices.values().stream(), clusterServices.values().stream());
        }

        @Override
        public void register(FeedService service) {
            UUID serviceId = service.getServerId();
            localServices.put(serviceId, service);
            
            getServices().map(FeedService::getServerId).filter(id->!id.equals(serviceId)).forEach(other->{
                replicate(serviceId, other);
                replicate(other, serviceId);
            });  
            
            service.setManager(this);
        }

        @Override
        public void deregister(FeedService service) {
            LOG.entry(service);
            UUID serverId = service.getServerId();
            synchronized(this) {
                localServices.remove(serverId);
                closeReplication(serverId);
                getHosts().forEach(host->host.closeReplication(serverId));
            }
            LOG.exit();
        }

        @Override
        public void dumpState(PrintWriter out) {
            out.println("On local host:");
            localServices.forEach((uuid, service) -> {
                out.format("Service id: %s\n", uuid);
                service.dumpState(out);
            });
            replicators.forEach(replicator-> {
                replicator.dumpState(out);
            });
        }

        @Override
        public Cluster getCluster() {
            return AbstractCluster.this;
        }
    }

    private final LocalHost localhost;
    private final Map<URI,Host> remoteHosts = new ConcurrentHashMap<>();
    private final Resolver<Host> hostResolver;
    private final URI localURI;
    
    public Optional<Host> getHost(UUID serviceId) {
        return getHosts()
            .filter(
                host->host.getLocalServices()
                    .map(FeedService::getServerId)
                    .anyMatch(id->id.equals(serviceId))
            )
            .findFirst();
    }
    
    @Override
    public LocalHost getLocalHost() {
        return localhost;
    }
    
    @Override
    public Stream<Host> getHosts() {
        try (StatusMap status = statusSupplier.get()) {
            return Stream.concat(
                Stream.of(localhost), 
                status.getEntries()
                    .filter(entry->entry.status.equals(HostStatus.UP))
                    .filter(entry->!entry.uri.equals(localURI))
                    .map(entry->remoteHosts.computeIfAbsent(entry.uri, k->hostResolver.resolve(entry.uri, JsonValue.EMPTY_JSON_OBJECT).orElseThrow(()->new RuntimeException("cannot resolve host URI " + entry.uri))))
            );
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    
    public Stream<FeedService> getCachedServices() {
        return Stream.concat(Stream.of(localhost), remoteHosts.values().stream()).flatMap(Host::getLocalServices);
    }
    
    public AbstractCluster(ExecutorService executor, URI localURI, Resolver<Host> hostResolver, Supplier<StatusMap> statusSupplier) {
        LOG.entry(executor, localURI, hostResolver, statusSupplier);
        this.executor = executor;
        this.hostResolver = hostResolver;
        this.localURI = localURI;
        this.statusSupplier = statusSupplier;
        try (StatusMap status = statusSupplier.get()) {
            status.setStatus(localURI, HostStatus.INTIALIZING);
            this.localhost = new LocalHostImpl();
            status.setStatus(localURI, HostStatus.UP);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        LOG.exit();
    }
    
    @Override
    public void close() {
        try (StatusMap status = statusSupplier.get()) {
            status.setStatus(localURI, HostStatus.DOWN);
            executor.shutdown();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }        
    }
    
    @Override
    public void register(FeedService clusterService) {
        LOG.entry(clusterService);
        clusterServices.put(clusterService.getServerId(), clusterService);
        clusterService.setManager(this);
        LOG.exit();
    }
    
    public void deregister(FeedService clusterService) {
        LOG.entry(clusterService);
        clusterServices.remove(clusterService.getServerId());
        LOG.exit();        
    }
    
    @Override
    public void dumpState(PrintWriter out) {
        out.println("localhost:");        
        localhost.dumpState(out);
        out.println("remote hosts:");
        remoteHosts.forEach((uri, host)->out.format("%s: %s\n", uri, host));
        out.println("cluster services:");
        clusterServices.forEach((uuid, service) -> out.format("%s: %s\n", uuid, service));
    }    
        
    public enum HostStatus {
        WAITING,
        INTIALIZING,
        UP,
        DOWN
    }
        
    public static interface StatusMap extends Closeable {        
        
        public static class Entry {
            public final URI uri;
            public final HostStatus status;
            public Entry(URI uri, HostStatus status) { this.uri = uri; this.status = status; }
        }
        
        void setStatus(URI host, HostStatus status);
        HostStatus getStatus(URI host);
        Stream<Entry> getEntries();
    }    
}
