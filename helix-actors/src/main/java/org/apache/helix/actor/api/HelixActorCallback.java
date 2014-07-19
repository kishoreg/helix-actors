package org.apache.helix.actor.api;

import java.util.UUID;

/**
 * Callback registered per-resource to handle messages sent via {@link HelixActor#send}
 */
public interface HelixActorCallback<T> {
    void onMessage(HelixActorScope scope, UUID messageId, T message);
}
