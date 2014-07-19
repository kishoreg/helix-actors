package org.apache.helix.actor.api;

import java.net.InetSocketAddress;
import java.util.Map;

public interface HelixActorResolver {

    /**
     * Given an Actor scope, return a map of instance name to actor socket address.
     */
    Map<String, InetSocketAddress> resolve(HelixActorScope scope);
}
