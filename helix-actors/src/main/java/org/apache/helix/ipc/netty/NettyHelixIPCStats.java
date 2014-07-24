package org.apache.helix.ipc.netty;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class NettyHelixIPCStats implements NettyHelixIPCStatsMBean {

    // Aggregate stats are over the last 5 minutes
    private static final int COLLECTION_PERIOD_SECONDS = 5 * 60;

    private final AtomicLong sendCount = new AtomicLong();
    private final AtomicLong ackCount = new AtomicLong();
    private final AtomicLong bytesCount = new AtomicLong();
    private final AtomicLong errorCount = new AtomicLong();
    private final AtomicLong channelOpenCount = new AtomicLong();
    private final DescriptiveStatistics sendStats = new SynchronizedDescriptiveStatistics(COLLECTION_PERIOD_SECONDS);
    private final DescriptiveStatistics ackStats = new SynchronizedDescriptiveStatistics(COLLECTION_PERIOD_SECONDS);
    private final DescriptiveStatistics bytesStats = new SynchronizedDescriptiveStatistics(COLLECTION_PERIOD_SECONDS);
    private final AtomicBoolean isShutdown = new AtomicBoolean(true);
    private final ScheduledExecutorService scheduler;

    private ScheduledFuture scheduledFuture;

    public NettyHelixIPCStats(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public long getSendCount() {
        return sendCount.get();
    }

    @Override
    public long getAckCount() {
        return ackCount.get();
    }

    @Override
    public long getBytesCount() {
        return bytesCount.get();
    }

    @Override
    public long getErrorCount() {
        return errorCount.get();
    }

    @Override
    public long getChannelOpenCount() {
        return errorCount.get();
    }

    @Override
    public double getSendsPerSecond() {
        return sendStats.getSum() / COLLECTION_PERIOD_SECONDS;
    }

    @Override
    public double getAcksPerSecond() {
        return ackStats.getSum() / COLLECTION_PERIOD_SECONDS;
    }

    @Override
    public double getBytesPerSecond() {
        return bytesStats.getSum() / COLLECTION_PERIOD_SECONDS;
    }

    public void countSend() {
        sendCount.incrementAndGet();
    }

    public void countAck() {
        ackCount.incrementAndGet();
    }

    public void countBytes(long numBytes) {
        bytesCount.addAndGet(numBytes);
    }

    public void countError() {
        errorCount.incrementAndGet();
    }

    public void countChannelOpen() {
        channelOpenCount.incrementAndGet();
    }

    public void start() {
        if (isShutdown.getAndSet(false)) {
            scheduledFuture = scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    sendStats.addValue(sendCount.get());
                    ackStats.addValue(ackCount.get());
                    bytesStats.addValue(bytesCount.get());
                }
            }, 0, 1, TimeUnit.SECONDS); // collect every 1 second (fixed)
        }
    }

    public void shutdown() {
        if (!isShutdown.getAndSet(true)) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
            }
        }
    }
}
