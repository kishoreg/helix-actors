package org.apache.helix.ipc;

import org.apache.helix.resolver.HelixMessageScope;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;

/**
 * Allows message passing among instances in Helix clusters.
 *
 * <p>
 *   Messages are sent asynchronously using {@link #send}, and handled by callbacks
 *   registered via {@link #registerCallback}
 * </p>
 */
public interface HelixIPCService {

    /** Starts service (must call before {@link #send}) */
    void start() throws Exception;

    /** Shuts down service and releases any resources */
    void shutdown() throws Exception;

    /**
     * Sends a message to one or more instances that map to a cluster scope.
     *
     * @param scope
     *  A scope which has already been resolved using {@link org.apache.helix.resolver.HelixResolver#resolve}
     * @param messageType
     *  The message type identifier, used in mapping callbacks / codecs
     * @param messageId
     *  A unique message identifier (which can be used in potential response messages)
     * @param message
     *  A typed message, for which there must be a registered message codec
     */
    void send(HelixMessageScope scope, int messageType, UUID messageId, Object message);

    /** Registers a callback for a given message type */
    void registerCallback(int messageType, HelixIPCCallback callback);

    /** Registers a codec for a given message type */
    void registerMessageCodec(int messageType, HelixIPCMessageCodec messageCodec);
}
