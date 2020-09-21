/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed.impl;

import com.softwareplumbers.feed.FeedExceptions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonWriter;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

/** Implementation of the Cluster interface using a shared file system.
 *
 * @author jonathan essex
 */
public abstract class FilesystemCluster extends CachingCluster {
    
    private static final XLogger LOG = XLoggerFactory.getXLogger(FilesystemCluster.class);
    static final Pattern FILENAME_MATCH_TEMPLATE = Pattern.compile("[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}");
    private final Path clusterDir;
    
    public FilesystemCluster(Path clusterDir) throws IOException {
        this.clusterDir = clusterDir;
        Files.createDirectories(clusterDir);
    }
    
    @Override
    public RegistryElement fetch(UUID remote) {
        LOG.entry(remote);
        try (InputStream is = Files.newInputStream(clusterDir.resolve(remote.toString())); JsonReader reader = Json.createReader(is)) {
            return LOG.exit(RegistryElement.fromJson(reader.readObject()));
        } catch (IOException ioe) {
            throw FeedExceptions.runtime(ioe);
        }
    }
    
    @Override 
    public Stream<RegistryElement> fetchAll() {
        LOG.entry();
        return LOG.exit(
            StreamSupport.stream(clusterDir.spliterator(), false)
                .map(path->path.getFileName().toString())
                .filter(filename->FILENAME_MATCH_TEMPLATE.matcher(filename).matches())
                .map(filename->fetch(UUID.fromString(filename)))
        );
    }
    

    @Override
    public void save(RegistryElement element) {
        LOG.entry(element);
        try (OutputStream os = Files.newOutputStream(clusterDir.resolve(element.serviceId.toString())); JsonWriter writer = Json.createWriter(os)) {
            writer.writeObject(element.toJson());
            LOG.exit();
        } catch (IOException ioe) {
            throw LOG.throwing(FeedExceptions.runtime(ioe));
        }
    }
    
    @Override 
    public void remove(UUID id) {
        LOG.entry(id);
        try {
            Files.deleteIfExists(clusterDir.resolve(id.toString()));
        } catch(IOException ioe) {
            throw LOG.throwing(FeedExceptions.runtime(ioe));            
        }
        LOG.exit();
    }
    
    private static class Lock implements AutoCloseable {
        public final FileChannel channel;
        public final FileLock lock;
        
        public Lock(Path clusterDir, UUID serverId) {
            LOG.entry(clusterDir, serverId);
            try {
            channel = FileChannel.open(clusterDir.resolve("lock"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                try {
                    lock = channel.lock();
                    channel.write(ByteBuffer.wrap(serverId.toString().getBytes()));
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
        public void close() throws Exception {
            LOG.entry();
            lock.release();
            channel.close();
            LOG.exit();
        }
        
    }
    
    @Override
    public AutoCloseable lock(UUID serverId) {
        LOG.entry(serverId);
        return LOG.exit(new Lock(clusterDir, serverId));
    }    
}
