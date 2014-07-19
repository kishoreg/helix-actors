package org.apache.helix.actor.netty;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class NettyHelixActorStats implements NettyHelixActorStatsMBean {

    // Aggregate stats are over the last 5 minutes
    private static final int COLLECTION_PERIOD_SECONDS = 5 * 60;
    private static final int SAMPLE_INTERVAL_SECONDS = 1;

    private final AtomicLong messageCount = new AtomicLong();
    private final AtomicLong bytesCount = new AtomicLong();
    private final AtomicLong errorCount = new AtomicLong();
    private final AtomicLong channelOpenCount = new AtomicLong();
    private final DescriptiveStatistics messageStats = new SynchronizedDescriptiveStatistics(COLLECTION_PERIOD_SECONDS);
    private final DescriptiveStatistics bytesStats = new SynchronizedDescriptiveStatistics(COLLECTION_PERIOD_SECONDS);
    private final AtomicBoolean isShutdown = new AtomicBoolean(true);
    private final ScheduledExecutorService scheduler;

    private ScheduledFuture scheduledFuture;

    public NettyHelixActorStats(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public long getMessageCount() {
        return messageCount.get();
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
    public double getMessagesPerSecond() {
        return messageStats.getSum() / COLLECTION_PERIOD_SECONDS;
    }

    @Override
    public double getBytesPerSecond() {
        return bytesStats.getSum() / COLLECTION_PERIOD_SECONDS;
    }

    public void countMessage() {
        messageCount.incrementAndGet();
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
                    messageStats.addValue(messageCount.get());
                    bytesStats.addValue(bytesCount.get());
                }
            }, 0, SAMPLE_INTERVAL_SECONDS, TimeUnit.SECONDS);
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