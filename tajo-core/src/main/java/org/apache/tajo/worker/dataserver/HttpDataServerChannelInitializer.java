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

package org.apache.tajo.worker.dataserver;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;

import org.apache.tajo.worker.dataserver.retriever.DataRetriever;

public class HttpDataServerChannelInitializer extends ChannelInitializer<Channel> {
  private final DataRetriever ret;

  public HttpDataServerChannelInitializer(DataRetriever ret) {
    this.ret = ret;
  }

  @Override
  protected void initChannel(Channel channel) throws Exception {
    // Create a default pipeline implementation.
    ChannelPipeline pipeline = channel.pipeline();

    // Uncomment the following line if you want HTTPS
    // SSLEngine engine =
    // SecureChatSslContextFactory.getServerContext().createSSLEngine();
    // engine.setUseClientMode(false);
    // pipeline.addLast("ssl", new SslHandler(engine));

    pipeline.addLast("decoder", new HttpRequestDecoder());
    //pipeline.addLast("aggregator", new HttpChunkAggregator(65536));
    pipeline.addLast("encoder", new HttpResponseEncoder());
    pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
    //pipeline.addLast("deflater", new HttpContentCompressor());
    pipeline.addLast("handler", new HttpDataServerHandler(ret));    
  }
}
