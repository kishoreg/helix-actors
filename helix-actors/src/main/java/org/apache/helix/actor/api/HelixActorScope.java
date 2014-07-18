package org.apache.helix.actor.api;

public class HelixActorScope {
    private String cluster;
    private String resource;
    private String partition;
    private String state;

    public String getCluster() { return cluster; }
    public String getResource() { return resource; }
    public String getPartition() { return partition; }
    public String getState() { return state; }

    public HelixActorScope setCluster(String cluster) { this.cluster = cluster; return this; }
    public HelixActorScope setResource(String resource) { this.resource = resource; return this; }
    public HelixActorScope setPartition(String partition) { this.partition = partition; return this; }
    public HelixActorScope setState(String state) { this.state = state; return this; }
}
