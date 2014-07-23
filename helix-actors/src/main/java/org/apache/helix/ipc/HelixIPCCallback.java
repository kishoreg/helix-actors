package org.apache.helix.ipc;

import org.apache.helix.resolver.HelixMessageScope;

import java.util.UUID;

/**
 * Callback registered per-resource to handle messages sent via {@link HelixIPCService#send}
 */
public interface HelixIPCCallback {
    void onMessage(HelixMessageScope scope, int messageType, UUID messageId, Object message);
}