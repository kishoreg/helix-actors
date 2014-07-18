package org.apache.helix.actor.api;

import org.apache.helix.model.Partition;

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Callback registered per-resource to handle messages sent via {@link HelixActor#send}
 */
public interface HelixActorCallback<T> {
    void onMessage(HelixActorScope scope, UUID messageId, T message);
}
