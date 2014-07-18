package org.apache.helix.actor.api;

import io.netty.buffer.ByteBuf;

/**
 * Encodes and decodes messages of type T
 */
public interface HelixActorMessageCodec<T> {
    /**
     * Encodes a typed message into a {@link io.netty.buffer.ByteBuf}.
     *
     * <p>
     *     {@link io.netty.buffer.ByteBuf#release()} will be called once on the
     *     return value, so if you want to ensure that it doesn't get
     *     reclaimed, call {@link io.netty.buffer.ByteBuf#retain()} before
     *     returning it.
     * </p>
     */
    ByteBuf encode(T message);

    /**
     * Decodes a typed message from a {@link io.netty.buffer.ByteBuf}.
     *
     * <p>
     *     The reader index will be positioned at the beginning of the message,
     *     and the next {@link io.netty.buffer.ByteBuf#readableBytes()} are the
     *     message.
     * </p>
     *
     * <p>
     *     For example, if T is String, and you want to generate a new object:
     *     <pre>
     *         byte[] bytes = new byte[message.readableBytes()];
     *         message.readBytes(bytes);
     *         return new String(bytes);
     *     </pre>
     * </p>
     */
    T decode(ByteBuf message);
}
