package org.apache.helix.ipc;

import org.apache.helix.resolver.HelixMessageScope;

import java.util.UUID;

/**
 * Callback registered per message type to handle messages sent via {@link AbstractHelixIPCService#send}
 *
 * @see AbstractHelixIPCService#registerCallback(int, HelixIPCCallback)
 */
public interface HelixIPCCallback {
    void onMessage(HelixMessageScope scope, UUID messageId, Object message);
}
