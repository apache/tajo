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

import com.google.protobuf.MessageLite;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;

class ProtoChannelInitializer extends ChannelInitializer<Channel> {
  private final MessageLite defaultInstance;
  private final ChannelHandler handler;
  private final int idleTimeSeconds;

  public ProtoChannelInitializer(ChannelHandler handler, MessageLite defaultInstance) {
    this(handler, defaultInstance, 0);
  }

  public ProtoChannelInitializer(ChannelHandler handler, MessageLite defaultInstance, int idleTimeSeconds) {
    this.handler = handler;
    this.defaultInstance = defaultInstance;
    this.idleTimeSeconds = idleTimeSeconds;
  }

  @Override
  protected void initChannel(Channel channel) throws Exception {
    ChannelPipeline pipeline = channel.pipeline();
    pipeline.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
    pipeline.addLast("protobufDecoder", new ProtobufDecoder(defaultInstance));
    pipeline.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
    pipeline.addLast("protobufEncoder", new ProtobufEncoder());
    pipeline.addLast("idleStateHandler", new IdleStateHandler(0, 0, idleTimeSeconds)); //zero is disabling
    pipeline.addLast("handler", handler);
  }
}
