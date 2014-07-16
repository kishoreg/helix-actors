package org.apache.helix.actor;

import org.apache.helix.model.Partition;

/**
 * Callback registered per-resource to handle messages sent via {@link HelixActor#send}
 */
public interface HelixActorCallback<T> {
    // TODO: Replace this with a context object w/ all metadata from message
    // TODO: not onMessage, something like onEvent
    void onMessage(Partition partition, String state, T message);
}
