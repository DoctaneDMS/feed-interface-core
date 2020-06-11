# Core Doctane Feed Interface

This project contains core interfaces and classes which abstract common interactions
with a messaging or chat system.
 
These classes are used both on the client side, usually to represent a remote 
system, and on the server side, where they represent a module implementing a set
of services which is exposed by the server.

## Package com.softwareplumbers.feed

This package contains mainly interface classes are used both client and server-side in order to
abstract operations on a messaging system. Key interfaces to start with are:

* Message - A message including Json metadata and an optional binary part
* Feed - An endpoint to which messages may be sent and from which messages are received
* FeedService - The core interface used to access and manipulate feeds and messages

## Package com.softwareplumbers.feed.impl

This package contains a generic implementation of various interfaces defined in the 
com.softwareplumbers.feed package. They are of use to avoid redundancy between different
implementations of FeedService. Implementers of a FeedService are not required
to use the implementations in this package; however, their use is encouraged in order to 
ensure consistent behavior.

## Package com.softwareplumbers.feed.impl.buffer

This packaged contains a glorified circular buffer for messages which can be used to help
'bridge the gap' between the doctane feed interface and some back-end.