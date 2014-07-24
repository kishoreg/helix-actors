package org.apache.helix.ipc;

import org.apache.helix.resolver.HelixMessageScope;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Base class implementation of {@link org.apache.helix.ipc.HelixIPCService}
 *
 * <p>
 *     Ensures that service is composed with all the necessary components.
 * </p>
 */
public abstract class AbstractHelixIPCService implements HelixIPCService {

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

    @Override
    public abstract void start() throws Exception;

    @Override
    public abstract void shutdown() throws Exception;

    @Override
    public abstract void send(HelixMessageScope scope, int messageType, UUID messageId, Object message);

    @Override
    public void registerCallback(int messageType, HelixIPCCallback callback) {
        this.callbacks.put(messageType, callback);
    }

    @Override
    public void registerMessageCodec(int messageType, HelixIPCMessageCodec messageCodec) {
        if (messageType < HelixIPCConstants.FIRST_CUSTOM_MESSAGE_TYPE) {
            throw new IllegalArgumentException("First allowed custom message type is "
                    + HelixIPCConstants.FIRST_CUSTOM_MESSAGE_TYPE);
        }
        this.messageCodecs.put(messageType, messageCodec);
    }
}
