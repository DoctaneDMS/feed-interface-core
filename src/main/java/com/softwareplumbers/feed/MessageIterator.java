/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.feed;

import com.softwareplumbers.feed.FeedExceptions.BaseRuntimeException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** A Closeable iterator over messages.
 *
 * Message iterators must be closed!! Fair question why this instead of Stream.
 * Answer: Stream does not have a .hasNext(), and an efficient hasNext is important
 * in some applications. Also programmers are WAY too used to not closing streams
 * in Java. The hope is that by introducing a specific interface we avoid this
 * default behavior.
 * 
 * @author jonathan
 */
public abstract class MessageIterator implements AutoCloseable, Iterator<Message> {

    private Runnable closeHandler;
    
    public MessageIterator(Runnable closeHandler) {
        this.closeHandler = closeHandler;
    }
    
    public Peekable peekable() {
        return new Peekable(this);
    }
    
    /** Release any resources associated with this MessageIterator.
     * 
     * The close handler will only be called once.
     * 
     */
    @Override
    public void close() {
        closeHandler.run();
        closeHandler = ()->{};
    }

    /** Convert iterator to a stream
     * 
     * @return A stream containing all the messages in this iterator.
     */
    public Stream<Message> toStream() {
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                this,
                Spliterator.ORDERED
            ), 
            false);        
    }
    
    /** Create a new message iterator which peeks at messages.
     * 
     * The supplied consumer will be invoked as each message is
     * by the returned iterator.
     * 
     * @param consumer function to process individual messages
     * @return A new iterator
     */
    public MessageIterator peek(Consumer<Message> consumer) {
        return new Chained(this, consumer);
    }

    private static class Delegator extends MessageIterator {
        private final Iterator<Message> base;
    
        public Delegator(Iterator<Message> base, Runnable closeHandler) {
            
            super(closeHandler);
            this.base = base;
        }

        @Override
        public boolean hasNext() {
            return base.hasNext();
        }

        @Override
        public Message next() {
            return base.next();
        }  
    }
    
    private static class DeferredError extends MessageIterator {
        private final BaseRuntimeException error;
        
        public DeferredError(BaseRuntimeException error) {
            super(()->{});
            this.error = error;
        }
        
        @Override
        public boolean hasNext() {
            throw error;
        }
        
        @Override 
        public Message next() {
            throw error;
        }
    }

    private static class Chained extends Delegator {
        
        private final Consumer<Message> consumer;
    
        public Chained(MessageIterator base, Consumer<Message> consumer) {
            super(base, base.closeHandler);
            this.consumer = consumer;
        }

        @Override
        public Message next() {
            Message result = super.next();
            consumer.accept(result);
            return result;
        }  
    }
    
    private static class Singleton extends MessageIterator {
        private Message message;
        
        public Singleton(Message message) {
            super(()->{});
            this.message = message;
        }
        
        @Override
        public boolean hasNext() {
            return message != null;
        }

        @Override
        public Message next() {
            Message result = message;
            message = null;
            return result;
        }  
    }
    
    private static class Sequence extends MessageIterator {
        private final MessageIterator[] seq;
        private int pos;
        
        private final void skip() {
            while (pos < seq.length && !seq[pos].hasNext()) {
                seq[pos].close();
                pos++;
            }            
        }
        
        public Sequence(MessageIterator... seq) {
            super(()->Stream.of(seq).forEach(MessageIterator::close));
            this.seq = seq;
            this.pos = 0;
            skip();
        }
        
        @Override
        public boolean hasNext() {
            return pos < seq.length;
        }

        @Override
        public Message next() {
            Message next = hasNext() ? seq[pos].next() : null;
            skip();
            return next;

        }       
    }
    
    private static class Merge extends MessageIterator {
        private final Peekable[] merged;
        
        @Override
        public void close() {
            for (Peekable source : merged) source.close();
        }
        
        public Merge(Stream<MessageIterator> sources) {
            super(()->{});
            merged = sources.map(MessageIterator::peekable).toArray(Peekable[]::new);
        }
        
        @Override
        public boolean hasNext() {
            return Stream.of(merged).anyMatch(MessageIterator::hasNext);
        }

        @Override
        public Message next() {
            return Stream.of(merged)
                .filter(MessageIterator::hasNext).min(Comparator.comparing(peekable->peekable.peek().get().getTimestamp()))
                .orElseThrow(()->new RuntimeException("called next() when no next item available"))
                .next();
        }       
    }    
    

    public static class Peekable extends MessageIterator {
        
        Message next;
        final MessageIterator base;
        
        public Peekable(MessageIterator base) {
            super(base::close);
            this.base = base;
            if (base.hasNext()) {
                next = base.next();
            } else {
                next = null;
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Message next() {
            Message result = next;
            next = base.hasNext() ? base.next() : null;
            return result;
        }
        
        public Optional<Message> peek() {
            return Optional.ofNullable(next);
        }
    
        @Override
        public Peekable peekable() {
            return this;
        }
    }
        
    /** Create a MessageIterator from another iterator, plus a handler to release resources.
     * 
     * @param messages Iterator over messages.
     * @param closeHandler Function to release resources.
     * @return A MessageIterator.
     */
    public static MessageIterator of(Iterator<Message> messages, Runnable closeHandler) {
        return new Delegator(messages, closeHandler);
    }
    
    /** Create a MessageIterator from a stream.
     * 
     * The stream's close method will be called when the returned iterator
     * is closed.
     * 
     * @param messages
     * @return 
     */
    public static MessageIterator of(Stream<Message> messages) {
        return new Delegator(messages.iterator(), ()->messages.close());
    }
    
    /** Create a MessageIterator over a single message.
     * 
     * @param message
     * @return 
     */
    public static MessageIterator of(Message message) {
        return new Singleton(message);
    }
    
    /** Create a message iterator over several iterators that will be processed in order.
     * 
     * @param iterators
     * @return 
     */
    public static MessageIterator of(MessageIterator... iterators) {
        return new Sequence(iterators);
    }
    
    public static MessageIterator defer(BaseRuntimeException error) {
        return new DeferredError(error);
    }
    
    public static MessageIterator merge(MessageIterator... sources) {
        return new Merge(Stream.of(sources));
    }
    
    public static MessageIterator merge(Stream<MessageIterator> sources) {
        return new Merge(sources); 
    }
        
}
