package org.apache.helix.ipc;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HelixIPCMessageCodecRegistry {
    private static final int FIRST_CUSTOM_MESSAGE_TYPE = 1024;

    private final ConcurrentMap<Integer, HelixIPCMessageCodec> registry
            = new ConcurrentHashMap<Integer, HelixIPCMessageCodec>();

    public void put(int messageType, HelixIPCMessageCodec codec) {
        if (messageType < FIRST_CUSTOM_MESSAGE_TYPE) {
            throw new IllegalArgumentException("First allowed custom message type is " + FIRST_CUSTOM_MESSAGE_TYPE);
        }
        registry.put(messageType, codec);
    }

    public HelixIPCMessageCodec get(int messageType) {
        return registry.get(messageType);
    }
}
