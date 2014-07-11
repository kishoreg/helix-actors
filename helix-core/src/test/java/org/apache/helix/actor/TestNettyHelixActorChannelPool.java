package org.apache.helix.actor;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.helix.TestHelper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class TestNettyHelixActorChannelPool {

    private static final int NUM_SERVERS = 4;

    private int[] ports;
    private DiscardServer[] servers;
    private EventLoopGroup eventLoopGroup;
    private NettyHelixActorChannelPool channelPool;

    @BeforeClass
    public void beforeClass() throws Exception {
        ports = new int[NUM_SERVERS];
        servers = new DiscardServer[NUM_SERVERS];
        for (int i = 0; i < NUM_SERVERS; i++) {
            ports[i] = TestHelper.getRandomPort();
            servers[i] = new DiscardServer(ports[i]);
            servers[i].start();
        }

        eventLoopGroup = new NioEventLoopGroup();
        channelPool = new NettyHelixActorChannelPool(new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        socketChannel.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf) throws Exception {
                                // Do nothing
                            }
                        });
                    }
                }));
    }

    @AfterClass
    public void afterClass() throws Exception {
        eventLoopGroup.shutdownGracefully();
        for (int i = 0; i < NUM_SERVERS; i++) {
            servers[i].shutdown();
        }
    }

    @Test
    public void testDeadChannels() throws Exception {
        Set<ChannelFuture> cfs = new HashSet<ChannelFuture>();
        InetSocketAddress address = new InetSocketAddress(ports[0]);

        // Get all channels
        for (int i = 0; i < 5; i++) {
            ChannelFuture cf = channelPool.getChannelFuture(address);
            cfs.add(cf);
        }

        // Close and return them
        for (ChannelFuture cf : cfs) {
            cf.channel().close();
            channelPool.releaseChannelFuture(address, cf);
        }

        // Get another channel (should work b/c pool re-grows)
        final CountDownLatch latch = new CountDownLatch(1);
        ChannelFuture cf = channelPool.getChannelFuture(address);
        Assert.assertNotNull(cf);
        Assert.assertTrue(cf.isSuccess());
        cf.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                try {
                    Assert.assertTrue(channelFuture.channel().isOpen());
                } finally {
                    latch.countDown();
                }
            }
        });
        latch.await();
    }

    private static class DiscardServer {
        private final int port;
        private NioEventLoopGroup eventLoopGroup;

        DiscardServer(int port) {
            this.port = port;
        }

        void start() {
            eventLoopGroup = new NioEventLoopGroup();

            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(eventLoopGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext channelHandlerContext,
                                                            ByteBuf byteBuf) throws Exception {
                                    // Do nothing
                                }
                            });
                        }
                    });

            bootstrap.bind(new InetSocketAddress(port));
        }

        void shutdown() {
            eventLoopGroup.shutdownGracefully();
        }
    }
}
