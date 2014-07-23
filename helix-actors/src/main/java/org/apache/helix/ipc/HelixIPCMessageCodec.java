package org.apache.helix.ipc;

import io.netty.buffer.ByteBuf;

/**
 * Encodes and decodes typed messages to and from {@link io.netty.buffer.ByteBuf}s
 */
public interface HelixIPCMessageCodec {
    /**
     * Encodes a typed message into a {@link io.netty.buffer.ByteBuf}.
     *
     * <p>
     *     {@link io.netty.buffer.ByteBuf#release()} will be called once on the
     *     return value, so if you want to ensure that it doesn't get
     *     reclaimed, call {@link io.netty.buffer.ByteBuf#retain()} before
     *     returning it.
     * </p>
     *
     * <p>
     *     N.b. This does not necessarily need to generate a new ByteBuf.
     *     Existing ByteBufs from a Netty pipeline may be used, for example.
     * </p>
     *
     * @see io.netty.buffer.ByteBuf#slice()
     * @see io.netty.buffer.CompositeByteBuf
     */
    ByteBuf encode(Object message);

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
     *     For example, if the return value is String, and you want to generate a new object:
     *     <pre>
     *         byte[] bytes = new byte[message.readableBytes()];
     *         message.readBytes(bytes);
     *         return new String(bytes);
     *     </pre>
     * </p>
     */
    Object decode(ByteBuf message);
}
