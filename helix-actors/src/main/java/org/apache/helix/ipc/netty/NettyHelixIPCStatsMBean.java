package org.apache.helix.ipc.netty;

public interface NettyHelixIPCStatsMBean {
    long getMessageCount();
    long getBytesCount();
    long getErrorCount();
    long getChannelOpenCount();
    double getMessagesPerSecond();
    double getBytesPerSecond();
}
