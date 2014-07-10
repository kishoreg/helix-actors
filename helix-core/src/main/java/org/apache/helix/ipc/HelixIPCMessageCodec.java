package org.apache.helix.ipc;

/**
 * TODO: Description
 */
public interface HelixIPCMessageCodec<T> {
    byte[] encode(T message);
    T decode(byte[] message);
}
