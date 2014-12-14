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

package org.apache.tajo.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.pullserver.retriever.FileChunk;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

/**
 * Fetcher fetches data from a given uri via HTTP protocol and stores them into
 * a specific file. It aims at asynchronous and efficient data transmit.
 */
public class Fetcher {

  private final static Log LOG = LogFactory.getLog(Fetcher.class);

  private final URI uri;
  private final FileChunk fileChunk;
  private final TajoConf conf;

  private final String host;
  private int port;
  private final boolean useLocalFile;

  private long startTime;
  private long finishTime;
  private long fileLen;
  private int messageReceiveCount;
  private TajoProtos.FetcherState state;

  private Bootstrap bootstrap;

  public Fetcher(TajoConf conf, URI uri, FileChunk chunk, EventLoopGroup loopGroup) {
    this.uri = uri;
    this.fileChunk = chunk;
    this.useLocalFile = !chunk.fromRemote();
    this.state = TajoProtos.FetcherState.FETCH_INIT;
    this.conf = conf;

    String scheme = uri.getScheme() == null ? "http" : uri.getScheme();
    this.host = uri.getHost() == null ? "localhost" : uri.getHost();
    this.port = uri.getPort();
    if (port == -1) {
      if (scheme.equalsIgnoreCase("http")) {
        this.port = 80;
      } else if (scheme.equalsIgnoreCase("https")) {
        this.port = 443;
      }
    }

    if (!useLocalFile) {
      bootstrap = new Bootstrap();
      bootstrap.group(loopGroup);
      bootstrap.channel(NioSocketChannel.class);
      bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000); // set 5 sec
      bootstrap.option(ChannelOption.SO_RCVBUF, 1048576); // set 1M
      bootstrap.option(ChannelOption.TCP_NODELAY, true);

      ChannelInitializer<Channel> initializer = new HttpClientChannelInitializer(fileChunk.getFile());
      bootstrap.handler(initializer);
    }
  }

  public long getStartTime() {
    return startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public long getFileLen() {
    return fileLen;
  }

  public TajoProtos.FetcherState getState() {
    return state;
  }

  public int getMessageReceiveCount() {
    return messageReceiveCount;
  }

  public FileChunk get() throws IOException {
    if (useLocalFile) {
      LOG.info("Get pseudo fetch from local host");
      startTime = System.currentTimeMillis();
      finishTime = System.currentTimeMillis();
      state = TajoProtos.FetcherState.FETCH_FINISHED;
      return fileChunk;
    }

    LOG.info("Get real fetch from remote host");
    this.startTime = System.currentTimeMillis();
    this.state = TajoProtos.FetcherState.FETCH_FETCHING;
    ChannelFuture future = null;
    try {
      future = bootstrap.clone().connect(new InetSocketAddress(host, port));

      // Wait until the connection attempt succeeds or fails.
      Channel channel = future.awaitUninterruptibly().channel();
      if (!future.isSuccess()) {
        future.channel().close();
        state = TajoProtos.FetcherState.FETCH_FAILED;
        throw new IOException(future.cause());
      }

      String query = uri.getPath()
          + (uri.getRawQuery() != null ? "?" + uri.getRawQuery() : "");
      // Prepare the HTTP request.
      HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, query);
      request.headers().add(HttpHeaders.Names.HOST, host);
      request.headers().add(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
      request.headers().add(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

      LOG.info("Status: " + getState() + ", URI:" + uri);
      // Send the HTTP request.
      ChannelFuture channelFuture = channel.writeAndFlush(request);

      // Wait for the server to close the connection.
      channel.closeFuture().awaitUninterruptibly();

      channelFuture.addListener(ChannelFutureListener.CLOSE);

      fileChunk.setLength(fileChunk.getFile().length());
      return fileChunk;
    } finally {
      if(future != null){
        // Close the channel to exit.
        future.channel().close();
      }

      this.finishTime = System.currentTimeMillis();
      LOG.info("Fetcher finished:" + (finishTime - startTime) + " ms, " + getState() + ", URI:" + uri);
    }
  }

  public URI getURI() {
    return this.uri;
  }

  class HttpClientHandler extends ChannelInboundHandlerAdapter {
    private final File file;
    private RandomAccessFile raf;
    private FileChannel fc;
    private long length = -1;

    public HttpClientHandler(File file) throws FileNotFoundException {
      this.file = file;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {

      messageReceiveCount++;
      try {
        if (msg instanceof HttpResponse) {

          HttpResponse response = (HttpResponse) msg;

          StringBuilder sb = new StringBuilder();
          if (LOG.isDebugEnabled()) {
            sb.append("STATUS: ").append(response.getStatus())
                .append(", VERSION: ").append(response.getProtocolVersion())
                .append(", HEADER: ");
          }
          if (!response.headers().names().isEmpty()) {
            for (String name : response.headers().names()) {
              for (String value : response.headers().getAll(name)) {
                if (LOG.isDebugEnabled()) {
                  sb.append(name).append(" = ").append(value);
                }
                if (this.length == -1 && name.equals("Content-Length")) {
                  this.length = Long.parseLong(value);
                }
              }
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug(sb.toString());
          }

          if (response.getStatus().code() == HttpResponseStatus.NO_CONTENT.code()) {
            LOG.warn("There are no data corresponding to the request");
            length = 0;
            return;
          } else if (response.getStatus().code() != HttpResponseStatus.OK.code()){
            LOG.error(response.getStatus().reasonPhrase());
            state = TajoProtos.FetcherState.FETCH_FAILED;
            return;
          }

          this.raf = new RandomAccessFile(file, "rw");
          this.fc = raf.getChannel();
        }
        if (msg instanceof HttpContent) {
          HttpContent httpContent = (HttpContent) msg;
          ByteBuf content = httpContent.content();
          if (content.isReadable()) {
            fc.write(content.nioBuffer());
          }
          
          if (msg instanceof LastHttpContent) {
            if(raf != null) {
              fileLen = file.length();
            }
            
            IOUtils.cleanup(LOG, fc, raf);
            finishTime = System.currentTimeMillis();
            state = TajoProtos.FetcherState.FETCH_FINISHED;
          }
        }
      } catch(Exception e) {
        LOG.warn(e.getMessage(), e);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
      if (cause instanceof ReadTimeoutException) {
        LOG.warn(cause);
      } else {
        LOG.error("Fetch failed :", cause);
      }

      // this fetching will be retry
      IOUtils.cleanup(LOG, fc, raf);
      if(ctx.channel().isActive()){
        ctx.channel().close();
      }
      finishTime = System.currentTimeMillis();
      state = TajoProtos.FetcherState.FETCH_FAILED;
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
      if(getState() != TajoProtos.FetcherState.FETCH_FINISHED){
        //channel is closed, but cannot complete fetcher
        finishTime = System.currentTimeMillis();
        state = TajoProtos.FetcherState.FETCH_FAILED;
      }
      IOUtils.cleanup(LOG, fc, raf);
      
      super.channelUnregistered(ctx);
    }
  }

  class HttpClientChannelInitializer extends ChannelInitializer<Channel> {
    private final File file;

    public HttpClientChannelInitializer(File file) {
      this.file = file;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();

      int maxChunkSize = conf.getIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_CHUNK_MAX_SIZE);
      int readTimeout = conf.getIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_READ_TIMEOUT);

      pipeline.addLast("codec", new HttpClientCodec(4096, 8192, maxChunkSize));
      pipeline.addLast("inflater", new HttpContentDecompressor());
      pipeline.addLast("timeout", new ReadTimeoutHandler(readTimeout, TimeUnit.SECONDS));
      pipeline.addLast("handler", new HttpClientHandler(file));
    }
  }
}
