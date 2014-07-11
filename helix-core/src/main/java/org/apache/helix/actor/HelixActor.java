package org.apache.helix.actor;

import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.model.Partition;

public interface HelixActor<T> extends ExternalViewChangeListener {
    void start();
    void shutdown();
    void send(Partition partition, String state, T message);
    void register(String resource, HelixActorCallback<T> callback);
}
