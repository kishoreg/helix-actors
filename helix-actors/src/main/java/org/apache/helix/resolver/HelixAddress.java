package org.apache.helix.resolver;

import java.net.InetSocketAddress;

public class HelixAddress {
    private final String instanceName;
    private final InetSocketAddress socketAddress;

    public HelixAddress(String instanceName, InetSocketAddress socketAddress) {
        this.instanceName = instanceName;
        this.socketAddress = socketAddress;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }
}
