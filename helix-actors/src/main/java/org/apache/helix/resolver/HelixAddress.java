package org.apache.helix.resolver;

import java.net.InetSocketAddress;

public class HelixAddress {

    private final HelixMessageScope scope;
    private final String instanceName;
    private final InetSocketAddress socketAddress;

    public HelixAddress(HelixMessageScope scope, String instanceName, InetSocketAddress socketAddress) {
        this.scope = scope;
        this.instanceName = instanceName;
        this.socketAddress = socketAddress;
    }

    public HelixMessageScope getScope() {
        return scope;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    @Override
    public String toString() {
        return instanceName + "@" + socketAddress;
    }
}
