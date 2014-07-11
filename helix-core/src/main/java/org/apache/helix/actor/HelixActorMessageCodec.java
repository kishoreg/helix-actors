package org.apache.helix.actor;

/**
 * Encodes and decodes messages of type T
 */
public interface HelixActorMessageCodec<T> {
    byte[] encode(T message);
    T decode(byte[] message);
}
