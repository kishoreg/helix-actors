package org.apache.helix.ipc;

import org.apache.helix.resolver.HelixAddress;
import org.apache.helix.resolver.HelixMessageScope;

import java.util.Set;
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
     */
    void send(Set<HelixAddress> destinations, int messageType, UUID messageId, Object message);

    /**
     * Sends an acknowledgement to the original sender for a given message ID
     */
    void ack(HelixAddress source, UUID messageId);

    /** Registers a callback for a given message type */
    void registerCallback(int messageType, HelixIPCCallback callback);

    /** Registers a callback for ACK messages (acknowledge receipt of original message ID) */
    void registerAckCallback(HelixIPCCallback callback);

    /** Registers a codec for a given message type */
    void registerMessageCodec(int messageType, HelixIPCMessageCodec messageCodec);
}
