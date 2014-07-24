package org.apache.helix.ipc;

import org.apache.helix.resolver.HelixMessageScope;

import java.util.UUID;

/**
 * Callback registered per message type to handle messages sent via {@link AbstractHelixIPCService#send}
 *
 * @see AbstractHelixIPCService#registerCallback(int, HelixIPCCallback)
 */
public interface HelixIPCCallback {

    /**
     * @param scope
     *  An unresolved scope (one can call
     *  {@link org.apache.helix.resolver.HelixResolver#resolve(org.apache.helix.resolver.HelixMessageScope)} on this)
     * @param messageId
     *  A unique message id, which may be used as the message id for a potential response
     * @param message
     *  The message decoded using the {@link org.apache.helix.ipc.HelixIPCMessageCodec} registered for the
     *  message type for which this callback is registered (via
     *  {@link org.apache.helix.ipc.HelixIPCService#registerMessageCodec(int, HelixIPCMessageCodec)})
     */
    void onMessage(HelixMessageScope scope, UUID messageId, Object message);
}
