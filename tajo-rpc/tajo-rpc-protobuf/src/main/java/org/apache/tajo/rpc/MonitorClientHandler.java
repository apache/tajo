/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.Charset;

/**
 * Triggers an {@link MonitorStateEvent} when a remote peer has not respond.
 * */

public class MonitorClientHandler extends ChannelInboundHandlerAdapter {
  private ByteBuf ping;
  private boolean enableMonitor;

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    // Initialize the message.
    ping = ctx.alloc().buffer(RpcConstants.PING_PACKET.length())
        .writeBytes(RpcConstants.PING_PACKET.getBytes(Charset.defaultCharset()));
    IdleStateHandler handler = ctx.pipeline().get(IdleStateHandler.class);
    if(handler != null && handler.getWriterIdleTimeInMillis() > 0) {
      enableMonitor = true;
    }
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    ping.release();
    super.channelInactive(ctx);
  }

  private boolean isPing(Object msg) {
    if(msg instanceof ByteBuf){
      return ByteBufUtil.equals(ping.duplicate(), ((ByteBuf)msg).duplicate());
    }
    return false;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if(enableMonitor && isPing(msg)){
      //ignore ping response
      ReferenceCountUtil.release(msg);
    } else {
      super.channelRead(ctx, msg);
    }
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (enableMonitor && evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      if (e.state() == IdleState.READER_IDLE && !e.isFirst()) {
        /* No response to ping request.*/
        ctx.fireUserEventTriggered(MonitorStateEvent.MONITOR_EXPIRED_STATE_EVENT);
      } else if (e.state() == IdleState.WRITER_IDLE) {
        /* send ping packet to remote server */
        ctx.writeAndFlush(ping.duplicate().retain());
      }
    }
    super.userEventTriggered(ctx, evt);
  }
}