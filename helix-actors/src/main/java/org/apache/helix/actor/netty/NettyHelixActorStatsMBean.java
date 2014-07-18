package org.apache.helix.actor.netty;

public interface NettyHelixActorStatsMBean {
    long getMessageCount();
    long getBytesCount();
    long getErrorCount();
    long getChannelOpenCount();
    double getMessagesPerSecond();
    double getBytesPerSecond();
}
