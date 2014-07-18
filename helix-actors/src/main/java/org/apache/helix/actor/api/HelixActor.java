package org.apache.helix.actor.api;

import java.util.Set;
import java.util.UUID;

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
public interface HelixActor<T> {
    void start() throws Exception;
    void shutdown() throws Exception;
    Set<UUID> send(String resource, String partition, String state, T message);
    void register(String resource, HelixActorCallback<T> callback);
}
