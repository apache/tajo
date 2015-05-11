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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.charset.Charset;

/**
 * MonitorServerHandler is a packet receiver for detecting server hangs
 * Reply response when a remote peer sent a ping packet.
 */

public class MonitorServerHandler extends ChannelInboundHandlerAdapter {
  private static final Log LOG = LogFactory.getLog(MonitorServerHandler.class);

  private ByteBuf ping;

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    // Initialize the message.
    ping = ctx.alloc().directBuffer(4).writeBytes(RpcConstants.PING_PACKET.getBytes(Charset.defaultCharset()));
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    ping.release();
    super.channelInactive(ctx);
  }

  private boolean isPing(Object msg) {
    if (msg instanceof ByteBuf) {
      return ByteBufUtil.equals(ping.duplicate(), ((ByteBuf) msg).duplicate());
    }

    return false;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (isPing(msg)) {
      /* reply to client */
      if(LOG.isDebugEnabled()){
        LOG.debug("reply to " + ctx.channel());
      }
      ctx.writeAndFlush(msg);
    } else {
      super.channelRead(ctx, msg);
    }
  }
}