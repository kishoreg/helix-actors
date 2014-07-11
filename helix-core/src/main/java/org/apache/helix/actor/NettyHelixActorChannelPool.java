package org.apache.helix.actor;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pools channels to other nodes in the cluster for IPC traffic.
 */
public class NettyHelixActorChannelPool {

    private static final int MAX_CONNECTIONS = 5;
    private static final long POOL_OPERATION_TIMEOUT_MILLIS = 1000;

    private final Bootstrap bootstrap;
    private final ConcurrentMap<InetSocketAddress, BlockingQueue<ChannelFuture>> channels;
    private final ConcurrentMap<InetSocketAddress, AtomicInteger> leases;

    public NettyHelixActorChannelPool(Bootstrap bootstrap) {
        this.bootstrap = bootstrap;
        this.channels = new ConcurrentHashMap<InetSocketAddress, BlockingQueue<ChannelFuture>>();
        this.leases = new ConcurrentHashMap<InetSocketAddress, AtomicInteger>();
    }

    public ChannelFuture getChannelFuture(InetSocketAddress address) throws InterruptedException {
        synchronized (channels) {
            // Get lease counter
            AtomicInteger lease = leases.get(address);
            if (lease == null) {
                lease = new AtomicInteger();
                leases.put(address, lease);
            }

            // Get channels (or rebuild if empty)
            BlockingQueue<ChannelFuture> queue = channels.get(address);
            if (queue == null || (lease.get() == 0 && queue.isEmpty())) {
                queue = new ArrayBlockingQueue<ChannelFuture>(MAX_CONNECTIONS);
                channels.put(address, queue);
                for (int i = 0; i < MAX_CONNECTIONS; i++) {
                    queue.offer(bootstrap.connect(address).sync(), POOL_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                }
            }

            // Check out channel (may block)
            ChannelFuture cf = queue.poll(POOL_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            lease.incrementAndGet();
            return cf;
        }
    }

    public void releaseChannelFuture(InetSocketAddress address, ChannelFuture cf) {
        synchronized (channels) {
            // Get lease counter
            AtomicInteger lease = leases.get(address);
            if (lease == null) {
                throw new IllegalArgumentException("No lease exists for address " + address);
            }

            // Get channels
            BlockingQueue<ChannelFuture> queue = channels.get(address);
            if (queue == null) {
                throw new IllegalArgumentException("No channel pool exists for address " + address);
            }

            // Return channel to pool if it's healthy (may block)
            try {
                if (cf != null && cf.isSuccess() && cf.channel().isOpen()) {
                    queue.offer(cf, POOL_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                }
                lease.decrementAndGet();
            } catch (InterruptedException e) {
                throw new IllegalStateException("Could not return channel to pool", e);
            }
        }
    }

    public void close() {
        synchronized (channels) {
            for (Map.Entry<InetSocketAddress, AtomicInteger> entry : leases.entrySet()) {
                if (entry.getValue().get() > 0) {
                    throw new IllegalStateException("Cannot close pool with outstanding connections for " + entry.getKey());
                }
            }

            for (Map.Entry<InetSocketAddress, BlockingQueue<ChannelFuture>> entry : channels.entrySet()) {
                for (ChannelFuture cf : entry.getValue()) {
                    cf.channel().close();
                }
            }
        }
    }
}
