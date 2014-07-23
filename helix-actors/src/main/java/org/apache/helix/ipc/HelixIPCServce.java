package org.apache.helix.ipc;

import org.apache.helix.resolver.HelixMessageScope;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;

/**
 * Allows message passing among instances in Helix clusters.
 *
 * <p>
 *   Messages are sent asynchronously using {@link #send}, and handled by callbacks registered via {@link #registerCallback}
 * </p>
 */
public interface HelixIPCServce {

    /**
     * Call this before sending any messages, and must be called before callbacks can fire
     */
    void start() throws Exception;

    /**
     * Shut down and release any resources
     */
    void shutdown() throws Exception;

    /**
     * Sends a message to one or more nodes, and return the number of messages sent
     */
    void send(HelixMessageScope scope,
              Map<String, InetSocketAddress> addresses,
              int messageType,
              UUID messageId,
              Object message);

    /**
     * Register a callback for a given message type.
     */
    void registerCallback(int messageType, HelixIPCCallback callback);

    /**
     * Registers a codec for a given message type (must be done before call to {@link #send})
     */
    void registerMessageCodec(int messageType, HelixIPCMessageCodec messageCodec);
}
