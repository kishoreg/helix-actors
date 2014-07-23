package org.apache.helix.ipc;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HelixIPCMessageCodecRegistry {

    private final ConcurrentMap<Integer, HelixIPCMessageCodec> registry
            = new ConcurrentHashMap<Integer, HelixIPCMessageCodec>();

    public void put(int messageType, HelixIPCMessageCodec codec) {
        registry.put(messageType, codec);
    }

    public HelixIPCMessageCodec get(int messageType) {
        return registry.get(messageType);
    }
}
