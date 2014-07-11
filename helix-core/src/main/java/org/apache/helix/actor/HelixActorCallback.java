package org.apache.helix.actor;

import org.apache.helix.model.Partition;

/**
 * Callback registered per-resource to handle messages sent via {@link NettyHelixActor#send}
 */
public interface HelixActorCallback<T> {

    /**
     * Called when a message is received for a particular resource.
     */
    void onMessage(Partition partition, String state, T message);
}
