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

package org.apache.tajo.pullserver;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.gson.Gson;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpHeaders.Values;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.exception.InvalidURLException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.pullserver.PullServerUtil.PullServerParams;
import org.apache.tajo.pullserver.retriever.FileChunk;
import org.apache.tajo.pullserver.retriever.IndexCacheKey;
import org.apache.tajo.rpc.NettyUtils;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.storage.index.bst.BSTIndex.BSTIndexReader;
import org.apache.tajo.util.TajoIdUtils;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class TajoPullServerService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(TajoPullServerService.class);

  private int port;
  private ServerBootstrap selector;
  private final ChannelGroup accepted = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private HttpChannelInitializer channelInitializer;
  private int sslFileBufferSize;
  private int maxUrlLength;

  private FileSystem localFS;

  /**
   * Should the shuffle use posix_fadvise calls to manage the OS cache during
   * sendfile
   */
  private boolean manageOsCache;
  private int readaheadLength;
  private ReadaheadPool readaheadPool = ReadaheadPool.getInstance();

  private LoadingCache<IndexCacheKey, BSTIndexReader> indexReaderCache = null;
  private int lowCacheHitCheckThreshold;

  private static final boolean STANDALONE;

  static {
    String standalone = System.getenv(PullServerConstants.PULLSERVER_STANDALONE_ENV_KEY);
    STANDALONE = !StringUtils.isEmpty(standalone) && standalone.equalsIgnoreCase(Boolean.TRUE.toString());
  }

  @Metrics(name="PullServerShuffleMetrics", about="PullServer output metrics", context="tajo")
  static class ShuffleMetrics implements GenericFutureListener<ChannelFuture> {
    @Metric({"OutputBytes","PullServer output in bytes"})
    MutableCounterLong shuffleOutputBytes;
    @Metric({"Failed","# of failed shuffle outputs"})
    MutableCounterInt shuffleOutputsFailed;
    @Metric({"Succeeded","# of succeeded shuffle outputs"})
    MutableCounterInt shuffleOutputsOK;
    @Metric({"Connections","# of current shuffle connections"})
    MutableGaugeInt shuffleConnections;

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (future.isSuccess()) {
        shuffleOutputsOK.incr();
      } else {
        shuffleOutputsFailed.incr();
      }
      shuffleConnections.decr();
    }
  }

  final ShuffleMetrics metrics;

  TajoPullServerService(MetricsSystem ms) {
    super(PullServerConstants.PULLSERVER_SERVICE_NAME);
    metrics = ms.register(new ShuffleMetrics());
  }

  @SuppressWarnings("UnusedDeclaration")
  public TajoPullServerService() {
    this(DefaultMetricsSystem.instance());
  }

  // TODO change AbstractService to throw InterruptedException
  @Override
  public void serviceInit(Configuration conf) throws Exception {
    if (!(conf instanceof TajoConf)) {
      throw new IllegalArgumentException("Configuration must be a TajoConf instance");
    }
    TajoConf tajoConf = (TajoConf) conf;

    manageOsCache = tajoConf.getBoolean(PullServerConstants.SHUFFLE_MANAGE_OS_CACHE,
        PullServerConstants.DEFAULT_SHUFFLE_MANAGE_OS_CACHE);

    readaheadLength = tajoConf.getInt(PullServerConstants.SHUFFLE_READAHEAD_BYTES,
        PullServerConstants.DEFAULT_SHUFFLE_READAHEAD_BYTES);

    int workerNum = tajoConf.getIntVar(ConfVars.SHUFFLE_RPC_SERVER_WORKER_THREAD_NUM);

    selector = NettyUtils.createServerBootstrap("TajoPullServerService", workerNum)
        .option(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.ALLOCATOR, NettyUtils.ALLOCATOR)
        .childOption(ChannelOption.TCP_NODELAY, true);

    localFS = new LocalFileSystem();

    maxUrlLength = tajoConf.getIntVar(ConfVars.PULLSERVER_FETCH_URL_MAX_LENGTH);
    LOG.info("Tajo PullServer initialized: readaheadLength=" + readaheadLength);

    ServerBootstrap bootstrap = selector.clone();
    try {
      channelInitializer = new HttpChannelInitializer(tajoConf);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    bootstrap.childHandler(channelInitializer)
      .channel(NioServerSocketChannel.class);

    port = tajoConf.getIntVar(ConfVars.PULLSERVER_PORT);
    ChannelFuture future = bootstrap.bind(port)
        .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE)
        .syncUninterruptibly();

    accepted.add(future.channel());
    port = ((InetSocketAddress)future.channel().localAddress()).getPort();
    tajoConf.set(ConfVars.PULLSERVER_PORT.varname, Integer.toString(port));
    LOG.info(getName() + " listening on port " + port);

    sslFileBufferSize = conf.getInt(PullServerConstants.SUFFLE_SSL_FILE_BUFFER_SIZE_KEY,
        PullServerConstants.DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE);

    int cacheSize = tajoConf.getIntVar(ConfVars.PULLSERVER_CACHE_SIZE);
    int cacheTimeout = tajoConf.getIntVar(ConfVars.PULLSERVER_CACHE_TIMEOUT);

    indexReaderCache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .expireAfterWrite(cacheTimeout, TimeUnit.MINUTES)
        .removalListener(removalListener)
        .build(
            new CacheLoader<IndexCacheKey, BSTIndexReader>() {
              @Override
              public BSTIndexReader load(IndexCacheKey key) throws Exception {
                return new BSTIndex(tajoConf).getIndexReader(new Path(key.getPath(), "index"));
              }
            }
        );
    lowCacheHitCheckThreshold = (int) (cacheSize * 0.1f);

    if (STANDALONE) {
      File pullServerPortFile = getPullServerPortFile();
      if (pullServerPortFile.exists()) {
        pullServerPortFile.delete();
      }
      pullServerPortFile.getParentFile().mkdirs();
      LOG.info("Write PullServerPort to " + pullServerPortFile);
      FileOutputStream out = null;
      try {
        out = new FileOutputStream(pullServerPortFile);
        out.write(("" + port).getBytes());
      } catch (Exception e) {
        LOG.fatal("PullServer exists cause can't write PullServer port to " + pullServerPortFile +
            ", " + e.getMessage(), e);
        System.exit(-1);
      } finally {
        IOUtils.closeStream(out);
      }
    }
    super.serviceInit(conf);
    LOG.info("TajoPullServerService started: port=" + port);
  }

  public static boolean isStandalone() {
    return STANDALONE;
  }

  private static File getPullServerPortFile() {
    String pullServerPortInfoFile = System.getenv("TAJO_PID_DIR");
    if (StringUtils.isEmpty(pullServerPortInfoFile)) {
      pullServerPortInfoFile = "/tmp";
    }
    return new File(pullServerPortInfoFile + "/pullserver.port");
  }

  // TODO change to get port from master or tajoConf
  public static int readPullServerPort() {
    FileInputStream in = null;
    try {
      File pullServerPortFile = getPullServerPortFile();

      if (!pullServerPortFile.exists() || pullServerPortFile.isDirectory()) {
        return -1;
      }
      in = new FileInputStream(pullServerPortFile);
      byte[] buf = new byte[1024];
      int readBytes = in.read(buf);
      return Integer.parseInt(new String(buf, 0, readBytes));
    } catch (IOException e) {
      LOG.fatal(e.getMessage(), e);
      return -1;
    } finally {
      IOUtils.closeStream(in);
    }
  }

  public int getPort() {
    return port;
  }

  @Override
  public void serviceStop() throws Exception {
    accepted.close();
    if (selector != null) {
      if (selector.group() != null) {
        selector.group().shutdownGracefully();
      }
      if (selector.childGroup() != null) {
        selector.childGroup().shutdownGracefully();
      }
    }

    if (channelInitializer != null) {
      channelInitializer.destroy();
    }

    localFS.close();
    indexReaderCache.invalidateAll();

    super.serviceStop();
  }

  public List<FileChunk> getFileChunks(TajoConf conf, LocalDirAllocator lDirAlloc, PullServerParams params)
      throws IOException, ExecutionException {
    return PullServerUtil.getFileChunks(conf, lDirAlloc, localFS, params, indexReaderCache,
        lowCacheHitCheckThreshold);
  }

  class HttpChannelInitializer extends ChannelInitializer<SocketChannel> {

    final PullServer PullServer;
    private SSLFactory sslFactory;

    public HttpChannelInitializer(TajoConf conf) throws Exception {
      PullServer = new PullServer(conf);
      if (conf.getBoolVar(ConfVars.SHUFFLE_SSL_ENABLED_KEY)) {
        sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, conf);
        sslFactory.init();
      }
    }

    public void destroy() {
      if (sslFactory != null) {
        sslFactory.destroy();
      }
    }

    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();
      if (sslFactory != null) {
        pipeline.addLast("ssl", new SslHandler(sslFactory.createSSLEngine()));
      }

      int maxChunkSize = getConfig().getInt(ConfVars.SHUFFLE_FETCHER_CHUNK_MAX_SIZE.varname,
          ConfVars.SHUFFLE_FETCHER_CHUNK_MAX_SIZE.defaultIntVal);
      pipeline.addLast("codec", new HttpServerCodec(maxUrlLength, 8192, maxChunkSize));
      pipeline.addLast("aggregator", new HttpObjectAggregator(1 << 16));
      pipeline.addLast("chunking", new ChunkedWriteHandler());
      pipeline.addLast("shuffle", PullServer);
      // TODO factor security manager into pipeline
      // TODO factor out encode/decode to permit binary shuffle
      // TODO factor out decode of index to permit alt. models
    }
  }

  @ChannelHandler.Sharable
  class PullServer extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final TajoConf conf;
    private final LocalDirAllocator lDirAlloc =
      new LocalDirAllocator(ConfVars.WORKER_TEMPORAL_DIR.varname);
    private final Gson gson = new Gson();

    public PullServer(TajoConf conf) throws IOException {
      this.conf = conf;

      // init local temporal dir
      lDirAlloc.getAllLocalPathsToRead(".", conf);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
      accepted.add(ctx.channel());

      if(LOG.isDebugEnabled()) {
        LOG.debug(String.format("Current number of shuffle connections (%d)", accepted.size()));
      }
      super.channelRegistered(ctx);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request)
            throws Exception {

      if (request.getDecoderResult().isFailure()) {
        LOG.error("Http decoding failed. ", request.getDecoderResult().cause());
        sendError(ctx, request.getDecoderResult().toString(), HttpResponseStatus.BAD_REQUEST);
        return;
      }

      if (request.getMethod() == HttpMethod.DELETE) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.NO_CONTENT);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);

        clearIndexCache(request.getUri());
        return;
      } else if (request.getMethod() != HttpMethod.GET) {
        sendError(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED);
        return;
      }

      // Parsing the URL into key-values
      try {
        final PullServerParams params = new PullServerParams(request.getUri());
        if (PullServerUtil.isChunkRequest(params.requestType())) {
          handleChunkRequest(ctx, request, params);
        } else {
          handleMetaRequest(ctx, request, params);
        }
      } catch (Throwable e) {
        LOG.error("Failed to handle request " + request.getUri());
        sendError(ctx, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        return;
      }
    }

    /**
     * Upon a request from TajoWorker, this method clears index cache for fetching data of an execution block.
     * It is called whenever an execution block is completed.
     *
     * @param uri query URI which indicates the execution block id
     * @throws IOException
     * @throws InvalidURLException
     */
    public void clearIndexCache(String uri)
        throws IOException, InvalidURLException {
      // Simply parse the given uri
      String[] tokens = uri.split("=");
      if (tokens.length != 2 || !tokens[0].equals("ebid")) {
        throw new IllegalArgumentException("invalid params: " + uri);
      }
      ExecutionBlockId ebId = TajoIdUtils.createExecutionBlockId(tokens[1]);
      String queryId = ebId.getQueryId().toString();
      String ebSeqId = Integer.toString(ebId.getId());
      List<IndexCacheKey> removed = new ArrayList<>();
      synchronized (indexReaderCache) {
        for (Entry<IndexCacheKey, BSTIndexReader> e : indexReaderCache.asMap().entrySet()) {
          IndexCacheKey key = e.getKey();
          if (key.getQueryId().equals(queryId) && key.getEbSeqId().equals(ebSeqId)) {
            e.getValue().forceClose();
            removed.add(e.getKey());
          }
        }
        indexReaderCache.invalidateAll(removed);
      }
      removed.clear();
      synchronized (waitForRemove) {
        for (Entry<IndexCacheKey, BSTIndexReader> e : waitForRemove.entrySet()) {
          IndexCacheKey key = e.getKey();
          if (key.getQueryId().equals(queryId) && key.getEbSeqId().equals(ebSeqId)) {
            e.getValue().forceClose();
            removed.add(e.getKey());
          }
        }
        for (IndexCacheKey eachKey : removed) {
          waitForRemove.remove(eachKey);
        }
      }
    }

    private void handleMetaRequest(ChannelHandlerContext ctx, FullHttpRequest request, final PullServerParams params)
        throws IOException, ExecutionException {
      final List<String> jsonMetas;
      try {
        jsonMetas = PullServerUtil.getJsonMeta(conf, lDirAlloc, localFS, params, gson, indexReaderCache,
            lowCacheHitCheckThreshold);
      } catch (FileNotFoundException e) {
        sendError(ctx, e.getMessage(), HttpResponseStatus.NO_CONTENT);
        return;
      } catch (IOException | IllegalArgumentException e) { // IOException, EOFException, IllegalArgumentException
        sendError(ctx, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        return;
      } catch (ExecutionException e) {
        // There are some problems in index cache
        throw new TajoInternalError(e.getCause());
      }

      FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.OK,
          Unpooled.copiedBuffer(gson.toJson(jsonMetas), CharsetUtil.UTF_8));
      response.headers().set(Names.CONTENT_TYPE, "application/json; charset=UTF-8");
      HttpHeaders.setContentLength(response, response.content().readableBytes());
      if (HttpHeaders.isKeepAlive(request)) {
        response.headers().set(Names.CONNECTION, Values.KEEP_ALIVE);
      }
      ChannelFuture writeFuture = ctx.writeAndFlush(response);

      // Decide whether to close the connection or not.
      if (!HttpHeaders.isKeepAlive(request)) {
        // Close the connection when the whole content is written out.
        writeFuture.addListener(ChannelFutureListener.CLOSE);
      }
    }

    private void handleChunkRequest(ChannelHandlerContext ctx, FullHttpRequest request, final PullServerParams params)
        throws IOException {
      final List<FileChunk> chunks;
      try {
        chunks = PullServerUtil.getFileChunks(conf, lDirAlloc, localFS, params, indexReaderCache,
            lowCacheHitCheckThreshold);
      } catch (FileNotFoundException e) {
        sendError(ctx, e.getMessage(), HttpResponseStatus.NO_CONTENT);
        return;
      } catch (IOException | IllegalArgumentException e) { // IOException, EOFException, IllegalArgumentException
        sendError(ctx, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        return;
      } catch (ExecutionException e) {
        // There are some problems in index cache
        throw new TajoInternalError(e.getCause());
      }

      // Write the content.
      if (chunks.size() == 0) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.NO_CONTENT);

        if (!HttpHeaders.isKeepAlive(request)) {
          ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        } else {
          response.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
          ctx.writeAndFlush(response);
        }
      } else {
        FileChunk[] file = chunks.toArray(new FileChunk[chunks.size()]);
        ChannelFuture writeFuture = null;
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK);
        long totalSize = 0;
        StringBuilder sb = new StringBuilder();
        for (FileChunk chunk : file) {
          totalSize += chunk.length();
          sb.append(Long.toString(chunk.length())).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        HttpHeaders.addHeader(response, PullServerConstants.CHUNK_LENGTH_HEADER_NAME, sb.toString());
        HttpHeaders.setContentLength(response, totalSize);

        if (HttpHeaders.isKeepAlive(request)) {
          response.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        }
        // Write the initial line and the header.
        writeFuture = ctx.write(response);

        for (FileChunk chunk : file) {
          writeFuture = sendFile(ctx, chunk);
          if (writeFuture == null) {
            sendError(ctx, HttpResponseStatus.NOT_FOUND);
            return;
          }
        }

        if (ctx.pipeline().get(SslHandler.class) == null) {
          writeFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        } else {
          ctx.flush();
        }

        // Decide whether to close the connection or not.
        if (!HttpHeaders.isKeepAlive(request)) {
          // Close the connection when the whole content is written out.
          writeFuture.addListener(ChannelFutureListener.CLOSE);
        }
      }
    }

    private ChannelFuture sendFile(ChannelHandlerContext ctx,
                                   FileChunk file) throws IOException {
      RandomAccessFile spill = null;      
      ChannelFuture writeFuture;
      try {
        spill = new RandomAccessFile(file.getFile(), "r");
        if (ctx.pipeline().get(SslHandler.class) == null) {
          final FadvisedFileRegion filePart = new FadvisedFileRegion(spill,
              file.startOffset(), file.length(), manageOsCache, readaheadLength,
              readaheadPool, file.getFile().getAbsolutePath());
          writeFuture = ctx.write(filePart);
          writeFuture.addListener(new FileCloseListener(filePart));
        } else {
          // HTTPS cannot be done with zero copy.
          final FadvisedChunkedFile chunk = new FadvisedChunkedFile(spill,
              file.startOffset(), file.length(), sslFileBufferSize,
              manageOsCache, readaheadLength, readaheadPool,
              file.getFile().getAbsolutePath());
          writeFuture = ctx.write(new HttpChunkedInput(chunk));
        }
      } catch (FileNotFoundException e) {
        LOG.fatal(file.getFile() + " not found");
        return null;
      } catch (Throwable e) {
        if (spill != null) {
          //should close a opening file
          spill.close();
        }
        return null;
      }
      metrics.shuffleConnections.incr();
      metrics.shuffleOutputBytes.incr(file.length()); // optimistic
      return writeFuture;
    }

    private void sendError(ChannelHandlerContext ctx,
        HttpResponseStatus status) {
      sendError(ctx, "", status);
    }

    private void sendError(ChannelHandlerContext ctx, String message,
        HttpResponseStatus status) {
      ByteBuf content = Unpooled.copiedBuffer(message, CharsetUtil.UTF_8);
      FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status, content);
      response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
      HttpHeaders.setContentLength(response, content.writerIndex());

      // Close the connection as soon as the error message is sent.
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
      LOG.error(cause.getMessage(), cause);
      if (ctx.channel().isOpen()) {
        ctx.channel().close();
      }
    }
  }

  // Temporal space to wait for the completion of all index lookup operations
  private final ConcurrentHashMap<IndexCacheKey, BSTIndexReader> waitForRemove = new ConcurrentHashMap<>();

  // RemovalListener is triggered when an item is removed from the index reader cache.
  // It closes index readers when they are not used anymore.
  // If they are still being used, they are moved to waitForRemove map to wait for other operations' completion.
  private final RemovalListener<IndexCacheKey, BSTIndexReader> removalListener = (removal) -> {
    BSTIndexReader reader = removal.getValue();
    if (reader.getReferenceNum() == 0) {
      try {
        reader.close(); // tear down properly
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      waitForRemove.remove(removal.getKey());
    } else {
      waitForRemove.put(removal.getKey(), reader);
    }
  };
}
