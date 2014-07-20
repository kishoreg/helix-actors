package org.apache.helix.actor.api;

import org.apache.helix.actor.resolver.HelixMessageScope;

import java.util.UUID;

/**
 * Callback registered per-resource to handle messages sent via {@link HelixActor#send}
 */
public interface HelixActorCallback<T> {
    void onMessage(HelixMessageScope scope, UUID messageId, T message);
}
