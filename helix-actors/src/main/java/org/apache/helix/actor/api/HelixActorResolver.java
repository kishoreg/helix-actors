package org.apache.helix.actor.api;

import java.net.InetSocketAddress;
import java.util.Map;

public interface HelixActorResolver {
    Map<String, InetSocketAddress> resolve(HelixActorScope cluster);
}
