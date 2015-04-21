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

import com.google.protobuf.ServiceException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@ChannelHandler.Sharable
public class MonitorClientHandler extends ChannelInboundHandlerAdapter {
  private static final Log LOG = LogFactory.getLog(MonitorClientHandler.class);

  private ByteBuf ping;

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    // Initialize the message.
    ping = ctx.alloc().buffer(4).writeBytes("test".getBytes());
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
    if(!isPing(msg)){
      super.channelRead(ctx, msg);
    } else {
      //ignore ping response
    }
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      if (e.state() == IdleState.READER_IDLE) {
        /* No response to ping request. may be server hangs */
        LOG.error("Did not receive ping. Might be server hangs");
        ctx.close();
        ctx.fireExceptionCaught(new ServiceException("No response to ping request: " + ctx.channel()));
      } else if (e.state() == IdleState.WRITER_IDLE) {
        /* ping packet*/
        ctx.writeAndFlush(ping.duplicate().retain());
      }
    }
    super.userEventTriggered(ctx, evt);
  }
}