package org.apache.helix.ipc.api;

import org.apache.helix.ipc.resolver.HelixMessageScope;

import java.util.UUID;

/**
 * Callback registered per-resource to handle messages sent via {@link HelixIPC#send}
 */
public interface HelixIPCCallback<T> {
    void onMessage(HelixMessageScope scope, UUID messageId, T message);
}
