package org.apache.helix.ipc;

import org.apache.helix.model.Partition;

/**
 * TODO: Description
 */
public interface HelixIPCCallback<T> {
    void onMessage(Partition partition, T message);
}
