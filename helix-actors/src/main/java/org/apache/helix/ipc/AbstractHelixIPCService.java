package org.apache.helix.ipc;

import org.apache.helix.resolver.HelixMessageScope;
import org.apache.helix.resolver.HelixResolver;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Allows message passing among instances in Helix clusters.
 *
 * <p>
 *   Messages are sent asynchronously using {@link #send}, and handled by callbacks registered via {@link #registerCallback}
 * </p>
 */
public abstract class AbstractHelixIPCService {

    protected final String instanceName;
    protected final int port;
    protected final ConcurrentMap<Integer, HelixIPCCallback> callbacks;
    protected final ConcurrentMap<Integer, HelixIPCMessageCodec> messageCodecs;

    /**
     * @param instanceName
     *  The Helix instance name on which this IPC service is running.
     * @param port
     *  The port on which to listen for messages
     */
    public AbstractHelixIPCService(String instanceName, int port) {
        this.instanceName = instanceName;
        this.port = port;
        this.callbacks = new ConcurrentHashMap<Integer, HelixIPCCallback>();
        this.messageCodecs = new ConcurrentHashMap<Integer, HelixIPCMessageCodec>();
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
    public abstract void send(HelixMessageScope scope,
                              Map<String, InetSocketAddress> addresses,
                              int messageType,
                              UUID messageId,
                              Object message);

    /**
     * Register a callback for a given message type.
     */
    public void registerCallback(int messageType, HelixIPCCallback callback) {
        this.callbacks.put(messageType, callback);
    }

    /**
     * Registers a codec for a given message type (must be done before call to {@link #send})
     */
    public void registerMessageCodec(int messageType, HelixIPCMessageCodec messageCodec) {
        if (messageType < HelixIPCConstants.FIRST_CUSTOM_MESSAGE_TYPE) {
            throw new IllegalArgumentException("First allowed custom message type is "
                    + HelixIPCConstants.FIRST_CUSTOM_MESSAGE_TYPE);
        }
        this.messageCodecs.put(messageType, messageCodec);
    }
}
