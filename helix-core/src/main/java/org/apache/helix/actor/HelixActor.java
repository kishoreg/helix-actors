package org.apache.helix.actor;

import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.model.Partition;

/**
 * A message passing actor that lives on a Helix instance.
 *
 * <p>
 *   Messages are sent asynchronously using {@link #send}, and handled by callbacks registered via {@link #register}
 * </p>
 *
 * @param <T>
 *   The message type
 */
public interface HelixActor<T> extends ExternalViewChangeListener, InstanceConfigChangeListener {
    void start();
    void shutdown();
    void send(Partition partition, String state, T message);
    void register(String resource, HelixActorCallback<T> callback);
}
