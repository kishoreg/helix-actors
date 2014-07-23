package org.apache.helix.ipc;

import org.apache.helix.resolver.HelixMessageScope;

import java.util.UUID;

/**
 * Callback registered per-resource to handle messages sent via {@link HelixIPCService#send}
 */
public interface HelixIPCCallback<T> {
    void onMessage(HelixMessageScope scope, UUID messageId, T message);
}
