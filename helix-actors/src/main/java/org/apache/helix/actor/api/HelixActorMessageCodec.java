package org.apache.helix.actor.api;

/**
 * Encodes and decodes messages of type T
 */
public interface HelixActorMessageCodec<T> {
    // TODO: Use ByteBuf instead of byte[]?
    byte[] encode(T message);
    T decode(byte[] message);
}
