package com.wavefront.agent.histogram;

import com.google.common.base.Preconditions;

import com.squareup.tape.ObjectQueue;

import javax.validation.constraints.NotNull;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Inbound handler streaming a netty channel out to a square tape.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
@ChannelHandler.Sharable
public class QueuingChannelHandler<T> extends SimpleChannelInboundHandler<Object> {
  private final ObjectQueue<T> tape;

  public QueuingChannelHandler(@NotNull ObjectQueue<T> tape) {
    Preconditions.checkNotNull(tape);
    this.tape = tape;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object t) throws Exception {
    if (t != null) {
      tape.add((T)t);
    }
  }
}
