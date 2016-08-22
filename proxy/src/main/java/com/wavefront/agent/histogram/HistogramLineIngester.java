package com.wavefront.agent.histogram;

import com.google.common.base.Charsets;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * Combination of StringLine and TCP ingester
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class HistogramLineIngester extends ChannelInitializer implements Runnable {
  /**
   * Default number of seconds before the channel idle timeout handler closes the connection.
   */
  private static final int CHANNEL_IDLE_TIMEOUT_IN_SECS_DEFAULT = (int) TimeUnit.DAYS.toSeconds(1);
  private static final int MAXIMUM_FRAME_LENGTH = 4096;
  private static final int MAXIMUM_OUTSTANDING_CONNECTIONS = 1024;

  private static final Logger logger = Logger.getLogger(HistogramLineIngester.class.getCanonicalName());

  // The final handlers to be installed.
  private final ConcurrentLinkedQueue<ChannelHandler> handlers;
  private final int port;


  public HistogramLineIngester(Collection<ChannelHandler> handlers, int port) {
    this.handlers = new ConcurrentLinkedQueue<>(handlers);
    this.port = port;
  }

  @Override
  public void run() {
    ServerBootstrap bootstrap = new ServerBootstrap();
    NioEventLoopGroup parent = new NioEventLoopGroup(1);
    NioEventLoopGroup children = new NioEventLoopGroup(handlers.size());

    try {
      bootstrap
          .group(parent, children)
          .channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_BACKLOG, MAXIMUM_OUTSTANDING_CONNECTIONS)
          .localAddress(port)
          .childHandler(this);

      ChannelFuture f = bootstrap.bind().sync();
      f.channel().closeFuture().sync();
    } catch (final InterruptedException e) {
      logger.log(Level.WARNING, "Interrupted");
      parent.shutdownGracefully();
      children.shutdownGracefully();
      logger.info("Listener on port " + String.valueOf(port) + " shut down");
    }
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    // Get handler
    ChannelHandler handler = handlers.poll();

    // Add decoders and timeout, add handler()
    ChannelPipeline pipeline = ch.pipeline();
    pipeline.addLast(
        new LineBasedFrameDecoder(MAXIMUM_FRAME_LENGTH, true, true),
        new StringDecoder(Charsets.UTF_8),
        new IdleStateHandler(CHANNEL_IDLE_TIMEOUT_IN_SECS_DEFAULT, 0, 0),
        new ChannelDuplexHandler() {
          @Override
          public void userEventTriggered(ChannelHandlerContext ctx,
                                         Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
              if (((IdleStateEvent) evt).state() == IdleState.READER_IDLE) {
                logger.warning("terminating connection to graphite client due to inactivity after " + CHANNEL_IDLE_TIMEOUT_IN_SECS_DEFAULT + "s: " + ctx.channel());
                ctx.close();
              }
            }
          }
        },
        handler);
  }
}
