package org.apache.helix.actor.api;

import java.net.InetSocketAddress;
import java.util.Set;

public interface HelixActorResolver {
    Set<InetSocketAddress> resolve(HelixActorScope cluster);
}
