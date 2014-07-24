package org.apache.helix.ipc.netty;

public interface NettyHelixIPCStatsMBean {
    long getSendCount();
    long getAckCount();
    long getBytesCount();
    long getErrorCount();
    long getChannelOpenCount();
    double getSendsPerSecond();
    double getAcksPerSecond();
    double getBytesPerSecond();
}
