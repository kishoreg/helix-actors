package org.apache.helix.ipc;

import org.apache.helix.resolver.HelixMessageScope;

import java.util.UUID;

/**
 * Allows message passing among instances in Helix clusters.
 *
 * <p>
 *   Messages are sent asynchronously using {@link #send}, and handled by callbacks registered via {@link #register}
 * </p>
 */
public interface HelixIPCService {
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
    int send(HelixMessageScope scope, int messageType, UUID messageId, Object message);

    /**
     * Register a callback.
     *
     * <p>
     *     Should be called before start.
     * </p>
     */
    void register(HelixIPCCallback callback);
}
