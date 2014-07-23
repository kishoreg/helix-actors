package org.apache.helix.ipc;

import org.apache.helix.resolver.HelixMessageScope;
import org.apache.helix.resolver.HelixResolver;

import java.util.UUID;

/**
 * Allows message passing among instances in Helix clusters.
 *
 * <p>
 *   Messages are sent asynchronously using {@link #send}, and handled by callbacks registered via {@link #register}
 * </p>
 */
public abstract class AbstractHelixIPCService {

    protected final String instanceName;
    protected final int port;
    protected final HelixIPCMessageCodec.Registry codecRegistry;
    protected final HelixResolver resolver;

    /**
     * @param instanceName
     *  The Helix instance name on which this IPC service is running.
     * @param port
     *  The port on which to listen for messages
     * @param codecRegistry
     *  Should have a codec registered for every message type passed to {@link #send}
     * @param resolver
     *  Resolves {@link HelixMessageScope}s to physical addresses.
     */
    public AbstractHelixIPCService(String instanceName,
                                   int port,
                                   HelixIPCMessageCodec.Registry codecRegistry,
                                   HelixResolver resolver) {
        this.instanceName = instanceName;
        this.port = port;
        this.codecRegistry = codecRegistry;
        this.resolver = resolver;
    }

    /**
     * Call this before sending any messages, and must be called before callbacks can fire
     */
    public abstract void start() throws Exception;

    /**
     * Shut down and release any resources
     */
    public abstract void shutdown() throws Exception;

    /**
     * Sends a message to one or more nodes, and return the number of messages sent
     */
    public abstract int send(HelixMessageScope scope, int messageType, UUID messageId, Object message);

    /**
     * Register a callback.
     *
     * <p>
     *     Should be called before start.
     * </p>
     */
    public abstract void register(int messageType, HelixIPCCallback callback);
}
