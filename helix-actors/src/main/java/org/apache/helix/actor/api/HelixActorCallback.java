package org.apache.helix.actor.api;

import org.apache.helix.model.Partition;

import java.util.UUID;

/**
 * Callback registered per-resource to handle messages sent via {@link HelixActor#send}
 */
public interface HelixActorCallback<T> {
    // TODO: Replace this with a context object w/ all metadata from message
    // TODO: not onMessage, something like onEvent
    void onMessage(Partition partition, String state, UUID messageId, T message);
}
