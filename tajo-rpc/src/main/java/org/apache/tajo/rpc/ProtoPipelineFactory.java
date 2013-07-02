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
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;

public class ProtoPipelineFactory implements ChannelPipelineFactory {
  private final ChannelUpstreamHandler handler;
  private final MessageLite defaultInstance;

  public ProtoPipelineFactory(ChannelUpstreamHandler handlerFactory,
      MessageLite defaultInstance) {
    this.handler = handlerFactory;
    this.defaultInstance = defaultInstance;
  }

  public ChannelPipeline getPipeline() throws Exception {
    ChannelPipeline p = Channels.pipeline();
    p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576*2, 0, 4,
        0, 4));
    p.addLast("protobufDecoder", new ProtobufDecoder(defaultInstance));
    p.addLast("frameEncoder", new LengthFieldPrepender(4));
    p.addLast("protobufEncoder", new ProtobufEncoder());
    p.addLast("handler", handler);
    return p;
  }
}
