package org.apache.helix.actor.api;

import io.netty.buffer.ByteBuf;

/**
 * Encodes and decodes messages of type T
 */
public interface HelixActorMessageCodec<T> {
    ByteBuf encode(T message);
    T decode(ByteBuf message);
}
