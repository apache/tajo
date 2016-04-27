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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.TajoProtos.FetcherState;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.pullserver.PullServerConstants;
import org.apache.tajo.pullserver.PullServerConstants.Param;
import org.apache.tajo.pullserver.PullServerUtil;
import org.apache.tajo.pullserver.PullServerUtil.PullServerParams;
import org.apache.tajo.pullserver.PullServerUtil.PullServerRequestURIBuilder;
import org.apache.tajo.pullserver.TajoPullServerService;
import org.apache.tajo.pullserver.retriever.FileChunk;
import org.apache.tajo.pullserver.retriever.FileChunkMeta;
import org.apache.tajo.rpc.NettyUtils;
import org.apache.tajo.storage.HashShuffleAppenderManager;
import org.apache.tajo.storage.StorageUtil;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * LocalFetcher retrieves locally stored data. Its behavior can be different according to the pull server is running
 * externally or internally.
 *
 * <ul>
 *   <li>When an internal pull server is running, local fetchers can retrieve data directly.</li>
 *   <li>When an external pull server is running,</li>
 *   <ul>
 *     <li>If the shuffle type is hash, local fetchers can still retrieve data directly.</li>
 *     <li>If the shuffle type is range, local fetchers need to get meta information of data via HTTP. Once the meta
 *     information is retrieved, they can read data directly.</li>
 *   </ul>
 * </ul>
 */
public class LocalFetcher extends AbstractFetcher {

  private final static Log LOG = LogFactory.getLog(LocalFetcher.class);

  private final TajoPullServerService pullServerService;

  private final String host;
  private int port;
  private final Bootstrap bootstrap;
  private final int maxUrlLength;
  private final List<FileChunkMeta> chunkMetas = new ArrayList<>();
  private final String tableName;
  private final FileSystem localFileSystem;
  private final LocalDirAllocator localDirAllocator;

  @VisibleForTesting
  public LocalFetcher(TajoConf conf, URI uri, String tableName) throws IOException {
    super(conf, uri);
    this.maxUrlLength = conf.getIntVar(ConfVars.PULLSERVER_FETCH_URL_MAX_LENGTH);
    this.tableName = tableName;
    this.localFileSystem = new LocalFileSystem();
    this.localDirAllocator = new LocalDirAllocator(ConfVars.WORKER_TEMPORAL_DIR.varname);
    this.pullServerService = null;

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

    bootstrap = new Bootstrap()
        .group(
            NettyUtils.getSharedEventLoopGroup(NettyUtils.GROUP.FETCHER,
                conf.getIntVar(ConfVars.SHUFFLE_RPC_CLIENT_WORKER_THREAD_NUM)))
        .channel(NioSocketChannel.class)
        .option(ChannelOption.ALLOCATOR, NettyUtils.ALLOCATOR)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
            conf.getIntVar(ConfVars.SHUFFLE_FETCHER_CONNECT_TIMEOUT) * 1000)
        .option(ChannelOption.SO_RCVBUF, 1048576) // set 1M
        .option(ChannelOption.TCP_NODELAY, true);
  }

  public LocalFetcher(TajoConf conf, URI uri, ExecutionBlockContext executionBlockContext, String tableName) {
    super(conf, uri);
    this.localFileSystem = executionBlockContext.getLocalFS();
    this.localDirAllocator = executionBlockContext.getLocalDirAllocator();
    this.maxUrlLength = conf.getIntVar(ConfVars.PULLSERVER_FETCH_URL_MAX_LENGTH);
    this.tableName = tableName;

    Optional<TajoPullServerService> optional = executionBlockContext.getSharedResource().getPullServerService();
    if (optional.isPresent()) {
      // local pull server service
      this.pullServerService = optional.get();
      this.host = null;
      this.bootstrap = null;

    } else if (PullServerUtil.useExternalPullServerService(conf)) {
      // external pull server service
      pullServerService = null;

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

      bootstrap = new Bootstrap()
          .group(
              NettyUtils.getSharedEventLoopGroup(NettyUtils.GROUP.FETCHER,
                  conf.getIntVar(ConfVars.SHUFFLE_RPC_CLIENT_WORKER_THREAD_NUM)))
          .channel(NioSocketChannel.class)
          .option(ChannelOption.ALLOCATOR, NettyUtils.ALLOCATOR)
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
              conf.getIntVar(ConfVars.SHUFFLE_FETCHER_CONNECT_TIMEOUT) * 1000)
          .option(ChannelOption.SO_RCVBUF, 1048576) // set 1M
          .option(ChannelOption.TCP_NODELAY, true);
    } else {
      endFetch(FetcherState.FETCH_FAILED);
      throw new TajoInternalError("Pull server service is not initialized");
    }
  }

  @Override
  public List<FileChunk> get() throws IOException {
    this.startTime = System.currentTimeMillis();
    return pullServerService != null ? getWithInternalPullServer() : getWithExternalPullServer();
  }

  private List<FileChunk> getWithInternalPullServer() throws IOException {
    final List<FileChunk> fileChunks = new ArrayList<>();
    PullServerParams params = new PullServerParams(uri.toString());
    try {
      fileChunks.addAll(pullServerService.getFileChunks(conf, localDirAllocator, params));
    } catch (ExecutionException e) {
      endFetch(FetcherState.FETCH_FAILED);
      throw new TajoInternalError(e);
    }
    fileChunks.stream().forEach(c -> c.setEbId(tableName));
    endFetch(FetcherState.FETCH_DATA_FINISHED);
    if (fileChunks.size() > 0) {
      fileLen = fileChunks.get(0).length();
      fileNum = 1;
    } else {
      fileNum = 0;
      fileLen = 0;
    }
    return fileChunks;
  }

  private List<FileChunk> getWithExternalPullServer() throws IOException {
    final PullServerParams params = new PullServerParams(uri.toString());
    final Path queryBaseDir = PullServerUtil.getBaseOutputDir(params.queryId(), params.ebId());

    if (PullServerUtil.isRangeShuffle(params.shuffleType())) {
      return getChunksForRangeShuffle(params, queryBaseDir);
    } else if (PullServerUtil.isHashShuffle(params.shuffleType())) {
      return getChunksForHashShuffle(params, queryBaseDir);
    } else {
      endFetch(FetcherState.FETCH_FAILED);
      throw new IllegalArgumentException("unknown shuffle type: " + params.shuffleType());
    }
  }

  private List<FileChunk> getChunksForHashShuffle(final PullServerParams params, final Path queryBaseDir)
      throws IOException {
    final List<FileChunk> fileChunks = new ArrayList<>();
    final String partId = params.partId();
    final long offset = params.offset();
    final long length = params.length();
    final int partParentId = HashShuffleAppenderManager.getPartParentId(Integer.parseInt(partId), conf);
    final Path partPath = StorageUtil.concatPath(queryBaseDir, "hash-shuffle", String.valueOf(partParentId), partId);

    if (!localDirAllocator.ifExists(partPath.toString(), conf)) {
      endFetch(FetcherState.FETCH_FAILED);
      throw new IOException("Hash shuffle or Scattered hash shuffle - file not exist: " + partPath);
    }
    final Path path = localFileSystem.makeQualified(localDirAllocator.getLocalPathToRead(partPath.toString(), conf));
    final File file = new File(path.toUri());
    final long startPos = (offset >= 0 && length >= 0) ? offset : 0;
    final long readLen = (offset >= 0 && length >= 0) ? length : file.length();

    if (startPos >= file.length()) {
      endFetch(FetcherState.FETCH_FAILED);
      throw new IOException("Start pos[" + startPos + "] great than file length [" + file.length() + "]");
    }
    if (readLen > 0) {
      final FileChunk chunk = new FileChunk(file, startPos, readLen);
      chunk.setEbId(tableName);
      chunk.setFromRemote(false);
      fileChunks.add(chunk);
      fileLen = chunk.length();
      fileNum = 1;
    }

    endFetch(FetcherState.FETCH_DATA_FINISHED);
    return fileChunks;
  }

  private List<FileChunk> getChunksForRangeShuffle(final PullServerParams params, final Path queryBaseDir)
      throws IOException {
    final List<FileChunk> fileChunks = new ArrayList<>();

    if (state == FetcherState.FETCH_INIT) {
      final ChannelInitializer<Channel> initializer = new HttpClientChannelInitializer();
      bootstrap.handler(initializer);
    }

    this.state = FetcherState.FETCH_META_FETCHING;
    ChannelFuture future = null;
    try {
      future = bootstrap.clone().connect(new InetSocketAddress(host, port))
          .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);

      // Wait until the connection attempt succeeds or fails.
      Channel channel = future.awaitUninterruptibly().channel();
      if (!future.isSuccess()) {
        endFetch(FetcherState.FETCH_FAILED);
        throw new IOException(future.cause());
      }

      for (URI eachURI : createChunkMetaRequestURIs(host, port, params)) {
        String query = eachURI.getPath()
            + (eachURI.getRawQuery() != null ? "?" + eachURI.getRawQuery() : "");
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, query);
        request.headers().set(HttpHeaders.Names.HOST, host);
        request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
        request.headers().set(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

        if(LOG.isDebugEnabled()) {
          LOG.debug("Status: " + getState() + ", URI:" + eachURI);
        }
        // Send the HTTP request.
        channel.writeAndFlush(request);
      }
      // Wait for the server to close the connection. throw exception if failed
      channel.closeFuture().syncUninterruptibly();

      if (!state.equals(FetcherState.FETCH_META_FINISHED)) {
        endFetch(FetcherState.FETCH_FAILED);
      } else {
        state = FetcherState.FETCH_DATA_FETCHING;
        fileLen = fileNum = 0;
        for (FileChunkMeta eachMeta : chunkMetas) {
          Path outputPath = StorageUtil.concatPath(queryBaseDir, eachMeta.getTaskId(), "output");
          if (!localDirAllocator.ifExists(outputPath.toString(), conf)) {
            LOG.warn("Range shuffle - file not exist. " + outputPath);
            continue;
          }
          Path path = localFileSystem.makeQualified(localDirAllocator.getLocalPathToRead(outputPath.toString(), conf));
          File file = new File(URI.create(path.toUri() + "/output"));
          FileChunk chunk = new FileChunk(file, eachMeta.getStartOffset(), eachMeta.getLength());
          chunk.setEbId(tableName);
          fileChunks.add(chunk);
          fileLen += chunk.length();
          fileNum++;
        }
        endFetch(FetcherState.FETCH_DATA_FINISHED);
      }

      return fileChunks;
    } finally {
      if(future != null && future.channel().isOpen()){
        // Close the channel to exit.
        future.channel().close().awaitUninterruptibly();
      }
    }
  }

  public class HttpClientHandler extends ChannelInboundHandlerAdapter {
    private int length = -1;
    private int totalReceivedContentLength = 0;
    private byte[] buf;
    private final Gson gson = new Gson();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {

      messageReceiveCount++;
      if (msg instanceof HttpResponse) {
        try {
          HttpResponse response = (HttpResponse) msg;

          StringBuilder sb = new StringBuilder();
          if (LOG.isDebugEnabled()) {
            sb.append("STATUS: ").append(response.getStatus()).append(", VERSION: ")
                .append(response.getProtocolVersion()).append(", HEADER: ");
          }
          if (!response.headers().names().isEmpty()) {
            for (String name : response.headers().names()) {
              for (String value : response.headers().getAll(name)) {
                if (LOG.isDebugEnabled()) {
                  sb.append(name).append(" = ").append(value);
                }
                if (this.length == -1 && name.equals("Content-Length")) {
                  this.length = Integer.parseInt(value);
                  if (buf == null || buf.length < this.length) {
                    buf = new byte[this.length];
                  }
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
          } else if (response.getStatus().code() != HttpResponseStatus.OK.code()) {
            LOG.error(response.getStatus().reasonPhrase());
            state = TajoProtos.FetcherState.FETCH_FAILED;
            return;
          }
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        } finally {
          ReferenceCountUtil.release(msg);
        }
      }

      if (msg instanceof HttpContent) {
        HttpContent httpContent = (HttpContent) msg;
        ByteBuf content = httpContent.content();

        if (state != FetcherState.FETCH_FAILED) {
          try {
            if (content.isReadable()) {
              int contentLength = content.readableBytes();
              content.readBytes(buf, totalReceivedContentLength, contentLength);
              totalReceivedContentLength += contentLength;
            }

            if (msg instanceof LastHttpContent) {
              if (totalReceivedContentLength == length) {
                state = FetcherState.FETCH_META_FINISHED;

                List<String> jsonMetas = gson.fromJson(new String(buf), List.class);
                for (String eachJson : jsonMetas) {
                  FileChunkMeta meta = gson.fromJson(eachJson, FileChunkMeta.class);
                  chunkMetas.add(meta);
                }
                totalReceivedContentLength = 0;
                length = -1;
              } else {
                endFetch(FetcherState.FETCH_FAILED);
                throw new IOException("Invalid fetch meta length: " + totalReceivedContentLength + ", expected length: "
                    + length);
              }
            }
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
          } finally {
            ReferenceCountUtil.release(msg);
          }
        } else {
          // http content contains the reason why the fetch failed.
          LOG.error(content.toString(Charset.defaultCharset()));
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
      if (cause instanceof ReadTimeoutException) {
        LOG.warn(cause.getMessage(), cause);
      } else {
        LOG.error("Fetch failed :", cause);
      }

      // this fetching will be retry
      finishTime = System.currentTimeMillis();
      state = TajoProtos.FetcherState.FETCH_FAILED;
      ctx.close();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
      if(getState() == FetcherState.FETCH_INIT || getState() == FetcherState.FETCH_META_FETCHING){
        //channel is closed, but cannot complete fetcher
        finishTime = System.currentTimeMillis();
        LOG.error("Channel closed by peer: " + ctx.channel());
        state = TajoProtos.FetcherState.FETCH_FAILED;
      }

      super.channelUnregistered(ctx);
    }
  }

  public class HttpClientChannelInitializer extends ChannelInitializer<Channel> {

    @Override
    protected void initChannel(Channel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();

      int maxChunkSize = conf.getIntVar(ConfVars.SHUFFLE_FETCHER_CHUNK_MAX_SIZE);
      int readTimeout = conf.getIntVar(ConfVars.SHUFFLE_FETCHER_READ_TIMEOUT);

      pipeline.addLast("codec", new HttpClientCodec(4096, 8192, maxChunkSize));
      pipeline.addLast("inflater", new HttpContentDecompressor());
      pipeline.addLast("timeout", new ReadTimeoutHandler(readTimeout, TimeUnit.SECONDS));
      pipeline.addLast("handler", new HttpClientHandler());
    }
  }

  private List<URI> createChunkMetaRequestURIs(String pullServerAddr, int pullServerPort, PullServerParams params) {
    final PullServerRequestURIBuilder builder = new PullServerRequestURIBuilder(pullServerAddr, pullServerPort, maxUrlLength);
    builder.setRequestType(PullServerConstants.META_REQUEST_PARAM_STRING)
        .setQueryId(params.queryId())
        .setShuffleType(params.shuffleType())
        .setEbId(params.ebId())
        .setPartId(params.partId());

    if (params.contains(Param.OFFSET)) {
      builder.setOffset(params.offset()).setLength(params.length());
    }

    if (PullServerUtil.isRangeShuffle(params.shuffleType())) {
      builder.setStartKeyBase64(params.startKey())
          .setEndKeyBase64(params.endKey())
          .setLastInclude(params.last());
    }

    if (params.contains(Param.TASK_ID)) {
      builder.setTaskAttemptIds(params.taskAttemptIds());
    }
    return builder.build(true);
  }
}
