package org.apache.helix.actor.api;

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
    /**
     * Call this before sending any messages, and must be called before callbacks can fire
     */
    void start() throws Exception;

    /**
     * Shut down and release any resources
     */
    void shutdown() throws Exception;

    /**
     * Sends a message to one or more nodes, and return the number of messages sent
     */
    int send(HelixActorScope scope, UUID messageId, T message);

    /**
     * Register a callback.
     *
     * <p>
     *     Should be called before start.
     * </p>
     */
    void register(HelixActorCallback<T> callback);
}
