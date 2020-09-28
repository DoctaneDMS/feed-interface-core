/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import javax.json.Json;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonWriter;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/** Implementation of the Cluster interface using a shared file system.
 *
 * @author jonathan essex
 */
public class FilesystemCluster extends AbstractCluster {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(FilesystemCluster.class);
    
    private static class HostStatusFile implements StatusMap {

        public final FileChannel channel;
        public final FileLock lock;
        public final Map<URI,HostStatus> content = new ConcurrentHashMap<>();
        public static final Lock localLock = new ReentrantLock();
        
        public HostStatusFile(Path path) {
            LOG.entry(path);
            try {
                Files.createDirectories(path.getParent());        
                channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
                try {
                    localLock.lock();
                    lock = channel.lock();
                    ByteBuffer buffer = ByteBuffer.allocate(10000);
                    while (channel.read(buffer) >= 0) {                            
                    }
                    content.clear();
                    if (buffer.position() > 0) {
                        try (ByteArrayInputStream bis = new ByteArrayInputStream(buffer.array()); JsonReader reader = Json.createReader(bis)) {
                            reader.readObject().forEach((k,v)->content.put(URI.create(k), HostStatus.valueOf(((JsonString)v).getString())));
                        }
                    } 
                    LOG.exit();
                } catch (IOException ioe) {
                    channel.close();
                    throw LOG.throwing(new RuntimeException(ioe));
                }
            } catch (IOException ioe) {
                throw LOG.throwing(new RuntimeException(ioe));                
            }            
        }
        
        @Override
        public void setStatus(URI host, HostStatus status) {
            LOG.entry(host, status);
            content.put(host, status);
            LOG.exit();
        }

        @Override
        public HostStatus getStatus(URI host) {
            LOG.entry(host);
            return LOG.exit(content.get(host));
        }

        @Override
        public Stream<StatusMap.Entry> getEntries() {
            LOG.entry();
            return LOG.exit(content.entrySet().stream().map(entry->new StatusMap.Entry(entry.getKey(), entry.getValue())));
        }

        @Override
        public void close() throws IOException {
            LOG.entry();
            try {  
                channel.position(0);
                try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); JsonWriter writer = Json.createWriter(bos)) {
                    JsonObjectBuilder builder = Json.createObjectBuilder();
                    content.forEach((k,v)->builder.add(k.toString(), v.toString()));
                    writer.writeObject(builder.build());
                    channel.write(ByteBuffer.wrap(bos.toByteArray()));
                }
                channel.force(true);
                lock.release();
                localLock.unlock();
                channel.close();
                LOG.exit();
            } catch (IOException ioe) {
                lock.release();
                localLock.unlock();
                channel.close();
                throw LOG.throwing(new RuntimeException(ioe));
            }
        }
        
    }
    
    public FilesystemCluster(ExecutorService executor, Path clusterStatus, URI localUri, Resolver<Host> clusterResolver) throws IOException {
        super(executor, localUri, clusterResolver, ()->new HostStatusFile(clusterStatus));
    }
    
    public FilesystemCluster(int threadPoolSize, String clusterStatus, String localUri, Resolver<Host> clusterResolver) throws IOException {
        super(Executors.newFixedThreadPool(threadPoolSize), URI.create(localUri), clusterResolver, ()->new HostStatusFile(Paths.get(clusterStatus)));
    }
}
