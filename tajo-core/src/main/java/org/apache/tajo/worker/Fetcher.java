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
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.channels.FileChannel;

import static org.jboss.netty.channel.Channels.pipeline;

/**
 * Fetcher fetches data from a given uri via HTTP protocol and stores them into
 * a specific file. It aims at asynchronous and efficient data transmit.
 */
public class Fetcher {

  private final static Log LOG = LogFactory.getLog(Fetcher.class);

  private final URI uri;
  private final File file;
  private final TajoConf conf;

  private final String host;
  private int port;

  private long startTime;
  private long finishTime;
  private long fileLen;
  private int messageReceiveCount;
  private TajoProtos.FetcherState state;
  private Timer timer;

  private ClientBootstrap bootstrap;

  public Fetcher(TajoConf conf, URI uri, File file, ClientSocketChannelFactory factory) {
    this.uri = uri;
    this.file = file;
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

    bootstrap = new ClientBootstrap(factory);
    bootstrap.setOption("connectTimeoutMillis", 5000L); // set 5 sec
    bootstrap.setOption("receiveBufferSize", 1048576); // set 1M
    bootstrap.setOption("tcpNoDelay", true);

    ChannelPipelineFactory pipelineFactory = new HttpClientPipelineFactory(file);
    bootstrap.setPipelineFactory(pipelineFactory);
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

  public File get() throws IOException {
    this.startTime = System.currentTimeMillis();
    this.state = TajoProtos.FetcherState.FETCH_FETCHING;
    ChannelFuture future = null;
    try {
      future = bootstrap.connect(new InetSocketAddress(host, port));

      // Wait until the connection attempt succeeds or fails.
      Channel channel = future.awaitUninterruptibly().getChannel();
      if (!future.isSuccess()) {
        future.getChannel().close();
        state = TajoProtos.FetcherState.FETCH_FAILED;
        throw new IOException(future.getCause());
      }

      String query = uri.getPath()
          + (uri.getRawQuery() != null ? "?" + uri.getRawQuery() : "");
      // Prepare the HTTP request.
      HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, query);
      request.setHeader(HttpHeaders.Names.HOST, host);
      request.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
      request.setHeader(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

      LOG.info("Status: " + getState() + ", URI:" + uri);
      // Send the HTTP request.
      ChannelFuture channelFuture = channel.write(request);

      // Wait for the server to close the connection.
      channel.getCloseFuture().awaitUninterruptibly();

      channelFuture.addListener(ChannelFutureListener.CLOSE);

      return file;
    } finally {
      if(future != null){
        // Close the channel to exit.
        future.getChannel().close();
      }

      this.finishTime = System.currentTimeMillis();
      LOG.info("Fetcher finished:" + (finishTime - startTime) + " ms, " + getState() + ", URI:" + uri);
      if (timer != null) {
        timer.stop();
      }
    }
  }

  public URI getURI() {
    return this.uri;
  }

  class HttpClientHandler extends SimpleChannelUpstreamHandler {
    private volatile boolean readingChunks;
    private final File file;
    private RandomAccessFile raf;
    private FileChannel fc;
    private long length = -1;

    public HttpClientHandler(File file) throws FileNotFoundException {
      this.file = file;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {

      messageReceiveCount++;
      try {
        if (!readingChunks && e.getMessage() instanceof HttpResponse) {

          HttpResponse response = (HttpResponse) e.getMessage();

          StringBuilder sb = new StringBuilder();
          if (LOG.isDebugEnabled()) {
            sb.append("STATUS: ").append(response.getStatus())
                .append(", VERSION: ").append(response.getProtocolVersion())
                .append(", HEADER: ");
          }
          if (!response.getHeaderNames().isEmpty()) {
            for (String name : response.getHeaderNames()) {
              for (String value : response.getHeaders(name)) {
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

          if (response.getStatus().getCode() == HttpResponseStatus.NO_CONTENT.getCode()) {
            LOG.warn("There are no data corresponding to the request");
            length = 0;
            return;
          } else if (response.getStatus().getCode() != HttpResponseStatus.OK.getCode()){
            LOG.error(response.getStatus().getReasonPhrase());
            state = TajoProtos.FetcherState.FETCH_FAILED;
            return;
          }

          this.raf = new RandomAccessFile(file, "rw");
          this.fc = raf.getChannel();

          if (response.isChunked()) {
            readingChunks = true;
          } else {
            ChannelBuffer content = response.getContent();
            if (content.readable()) {
              fc.write(content.toByteBuffer());
            }
          }
        } else {
          HttpChunk chunk = (HttpChunk) e.getMessage();
          if (chunk.isLast()) {
            readingChunks = false;
            long fileLength = file.length();
            if (fileLength == length) {
              LOG.info("Data fetch is done (total received bytes: " + fileLength
                  + ")");
            } else {
              LOG.info("Data fetch is done, but cannot get all data "
                  + "(received/total: " + fileLength + "/" + length + ")");
            }
          } else {
            if(fc != null){
              fc.write(chunk.getContent().toByteBuffer());
            }
          }
        }
      } finally {
        if(raf != null) {
          fileLen = file.length();
        }

        if(fileLen == length){
          IOUtils.cleanup(LOG, fc, raf);
          finishTime = System.currentTimeMillis();
          state = TajoProtos.FetcherState.FETCH_FINISHED;
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {
      if (e.getCause() instanceof ReadTimeoutException) {
        LOG.warn(e.getCause());
      } else {
        LOG.error("Fetch failed :", e.getCause());
      }

      // this fetching will be retry
      IOUtils.cleanup(LOG, fc, raf);
      if(ctx.getChannel().isConnected()){
        ctx.getChannel().close();
      }
      finishTime = System.currentTimeMillis();
      state = TajoProtos.FetcherState.FETCH_FAILED;
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      super.channelDisconnected(ctx, e);

      if(getState() != TajoProtos.FetcherState.FETCH_FINISHED){
        //channel is closed, but cannot complete fetcher
        finishTime = System.currentTimeMillis();
        state = TajoProtos.FetcherState.FETCH_FAILED;
      }
      IOUtils.cleanup(LOG, fc, raf);
    }
  }

  class HttpClientPipelineFactory implements
      ChannelPipelineFactory {
    private final File file;

    public HttpClientPipelineFactory(File file) {
      this.file = file;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = pipeline();

      timer = new HashedWheelTimer();

      int maxChunkSize = conf.getIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_CHUNK_MAX_SIZE);
      int readTimeout = conf.getIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_READ_TIMEOUT);

      pipeline.addLast("codec", new HttpClientCodec(4096, 8192, maxChunkSize));
      pipeline.addLast("inflater", new HttpContentDecompressor());
      pipeline.addLast("timeout", new ReadTimeoutHandler(timer, readTimeout));
      pipeline.addLast("handler", new HttpClientHandler(file));
      return pipeline;
    }
  }
}
