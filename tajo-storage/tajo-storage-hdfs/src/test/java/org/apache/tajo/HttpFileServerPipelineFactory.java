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

package org.apache.tajo;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;

import static org.jboss.netty.channel.Channels.pipeline;

// Uncomment the following lines if you want HTTPS
//import javax.net.ssl.SSLEngine;
//import org.jboss.netty.example.securechat.SecureChatSslContextFactory;
//import org.jboss.netty.handler.ssl.SslHandler;

//this class is copied from HttpStaticFileServerPipelineFactory.java of netty 3.6
public class HttpFileServerPipelineFactory implements ChannelPipelineFactory {
  public ChannelPipeline getPipeline() throws Exception {
    // Create a default pipeline implementation.
    ChannelPipeline pipeline = pipeline();

    // Uncomment the following lines if you want HTTPS
    //SSLEngine engine = SecureChatSslContextFactory.getServerContext().createSSLEngine();
    //engine.setUseClientMode(false);
    //pipeline.addLast("ssl", new SslHandler(engine));

    pipeline.addLast("decoder", new HttpRequestDecoder());
    pipeline.addLast("aggregator", new HttpChunkAggregator(65536));
    pipeline.addLast("encoder", new HttpResponseEncoder());
    pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());

    pipeline.addLast("handler", new HttpFileServerHandler());
    return pipeline;
  }
}