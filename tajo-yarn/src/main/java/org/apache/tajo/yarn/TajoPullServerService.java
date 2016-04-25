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

package org.apache.tajo.yarn;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.exception.InvalidURLException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.pullserver.PullServerConstants;
import org.apache.tajo.pullserver.PullServerUtil;
import org.apache.tajo.pullserver.PullServerUtil.PullServerParams;
import org.apache.tajo.pullserver.retriever.FileChunk;
import org.apache.tajo.pullserver.retriever.IndexCacheKey;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.storage.index.bst.BSTIndex.BSTIndexReader;
import org.apache.tajo.util.SizeOf;
import org.apache.tajo.util.TajoIdUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.codec.http.HttpHeaders.Names;
import org.jboss.netty.handler.codec.http.HttpHeaders.Values;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.jboss.netty.util.CharsetUtil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.*;

import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class TajoPullServerService extends AuxiliaryService {

  private static final Log LOG = LogFactory.getLog(TajoPullServerService.class);

  private TajoConf tajoConf;

  private int port;
  private ChannelFactory selector;
  private final ChannelGroup accepted = new DefaultChannelGroup("Pull server group");
  private HttpChannelInitializer channelInitializer;
  private int sslFileBufferSize;
  private int maxUrlLength;

  private ApplicationId appId;
  private FileSystem localFS;

  /**
   * Should the shuffle use posix_fadvise calls to manage the OS cache during
   * sendfile
   */
  private boolean manageOsCache;
  private int readaheadLength;
  private ReadaheadPool readaheadPool = ReadaheadPool.getInstance();

  private static final Map<String,String> userRsrc =
          new ConcurrentHashMap<>();
  private String userName;

  private LoadingCache<IndexCacheKey, BSTIndexReader> indexReaderCache = null;
  private int lowCacheHitCheckThreshold;

  @Metrics(name="PullServerShuffleMetrics", about="PullServer output metrics", context="tajo")
  static class ShuffleMetrics implements ChannelFutureListener {
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

  @Override
  public void initializeApplication(ApplicationInitializationContext context) {
    // TODO these bytes should be versioned
    // TODO: Once SHuffle is out of NM, this can use MR APIs
    String user = context.getUser();
    ApplicationId appId = context.getApplicationId();
    //    ByteBuffer secret = context.getApplicationDataForService();
    userRsrc.put(appId.toString(), user);
  }

  @Override
  public void stopApplication(ApplicationTerminationContext context) {
    userRsrc.remove(context.getApplicationId().toString());
  }

  // TODO change AbstractService to throw InterruptedException
  @Override
  public void serviceInit(Configuration conf) throws Exception {
    tajoConf = new TajoConf(conf);

    manageOsCache = tajoConf.getBoolean(PullServerConstants.SHUFFLE_MANAGE_OS_CACHE,
        PullServerConstants.DEFAULT_SHUFFLE_MANAGE_OS_CACHE);

    readaheadLength = tajoConf.getInt(PullServerConstants.SHUFFLE_READAHEAD_BYTES,
        PullServerConstants.DEFAULT_SHUFFLE_READAHEAD_BYTES);

    int workerNum = tajoConf.getIntVar(ConfVars.SHUFFLE_RPC_SERVER_WORKER_THREAD_NUM);

    ThreadFactory bossFactory = new ThreadFactoryBuilder()
        .setNameFormat("TajoPullServerService Netty Boss #%d")
        .build();
    ThreadFactory workerFactory = new ThreadFactoryBuilder()
        .setNameFormat("TajoPullServerService Netty Worker #%d")
        .build();
    selector = new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(bossFactory),
        Executors.newCachedThreadPool(workerFactory),
        workerNum);

    localFS = new LocalFileSystem();

    maxUrlLength = tajoConf.getIntVar(ConfVars.PULLSERVER_FETCH_URL_MAX_LENGTH);

    LOG.info("Tajo PullServer initialized: readaheadLength=" + readaheadLength);

    ServerBootstrap bootstrap = new ServerBootstrap(selector);
    try {
      channelInitializer = new HttpChannelInitializer(tajoConf);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    bootstrap.setPipelineFactory(channelInitializer);

    port = tajoConf.getIntVar(ConfVars.PULLSERVER_PORT);
    Channel ch = bootstrap.bind(new InetSocketAddress(port));

    accepted.add(ch);
    port = ((InetSocketAddress)ch.getLocalAddress()).getPort();
    tajoConf.set(ConfVars.PULLSERVER_PORT.varname, Integer.toString(port));
    LOG.info(getName() + " listening on port " + port);

    sslFileBufferSize = tajoConf.getInt(PullServerConstants.SUFFLE_SSL_FILE_BUFFER_SIZE_KEY,
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

    super.serviceInit(tajoConf);
    LOG.info("TajoPullServerService started: port=" + port);
  }

  @Override
  public void serviceStop() throws Exception {
    // TODO: check this wait
    accepted.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
    if (selector != null) {
      ServerBootstrap bootstrap = new ServerBootstrap(selector);
      bootstrap.releaseExternalResources();
    }

    if (channelInitializer != null) {
      channelInitializer.destroy();
    }

    localFS.close();
    indexReaderCache.invalidateAll();

    super.serviceStop();
  }

  @VisibleForTesting
  public int getPort() {
    return port;
  }

  @Override
  public ByteBuffer getMetaData() {
    try {
      return serializeMetaData(port);
    } catch (IOException e) {
      LOG.error("Error during getMeta", e);
      // TODO add API to AuxiliaryServices to report failures
      return null;
    }
  }

  /**
   * Serialize the shuffle port into a ByteBuffer for use later on.
   * @param port the port to be sent to the ApplciationMaster
   * @return the serialized form of the port.
   */
  public static ByteBuffer serializeMetaData(int port) throws IOException {
    //TODO these bytes should be versioned
    return ByteBuffer.allocate(SizeOf.SIZE_OF_INT).putInt(port);
  }

  class HttpChannelInitializer implements ChannelPipelineFactory {

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
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = Channels.pipeline();
      if (sslFactory != null) {
        pipeline.addLast("ssl", new SslHandler(sslFactory.createSSLEngine()));
      }
      int maxChunkSize = getConfig().getInt(ConfVars.SHUFFLE_FETCHER_CHUNK_MAX_SIZE.varname,
          ConfVars.SHUFFLE_FETCHER_CHUNK_MAX_SIZE.defaultIntVal);
      pipeline.addLast("codec", new HttpServerCodec(maxUrlLength, 8192, maxChunkSize));
      pipeline.addLast("aggregator", new HttpChunkAggregator(1 << 16));
      pipeline.addLast("chunking", new ChunkedWriteHandler());
      pipeline.addLast("shuffle", PullServer);
      return pipeline;
      // TODO factor security manager into pipeline
      // TODO factor out encode/decode to permit binary shuffle
      // TODO factor out decode of index to permit alt. models
    }
  }

  @ChannelHandler.Sharable
  class PullServer extends SimpleChannelUpstreamHandler {

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
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent evt) throws Exception {
      accepted.add(evt.getChannel());

      if(LOG.isDebugEnabled()) {
        LOG.debug(String.format("Current number of shuffle connections (%d)", accepted.size()));
      }
      super.channelOpen(ctx, evt);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent evt)
            throws Exception {

      HttpRequest request = (HttpRequest) evt.getMessage();
      Channel ch = evt.getChannel();

      if (request.getMethod() == HttpMethod.DELETE) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
        ch.write(response).addListener(ChannelFutureListener.CLOSE);

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

    private void handleMetaRequest(ChannelHandlerContext ctx, HttpRequest request, final PullServerParams params)
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

      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK);
      response.setContent(ChannelBuffers.copiedBuffer(gson.toJson(jsonMetas), CharsetUtil.UTF_8));
      response.setHeader(Names.CONTENT_TYPE, "application/json; charset=UTF-8");
      HttpHeaders.setContentLength(response, response.getContent().readableBytes());
      if (HttpHeaders.isKeepAlive(request)) {
        response.setHeader(Names.CONNECTION, Values.KEEP_ALIVE);
      }
      ChannelFuture writeFuture = ctx.getChannel().write(response);

      // Decide whether to close the connection or not.
      if (!HttpHeaders.isKeepAlive(request)) {
        // Close the connection when the whole content is written out.
        writeFuture.addListener(ChannelFutureListener.CLOSE);
      }
    }

    private void handleChunkRequest(ChannelHandlerContext ctx, HttpRequest request, final PullServerParams params)
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
      final Channel ch = ctx.getChannel();
      if (chunks.size() == 0) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.NO_CONTENT);

        if (!HttpHeaders.isKeepAlive(request)) {
          ch.write(response).addListener(ChannelFutureListener.CLOSE);
        } else {
          response.setHeader(Names.CONNECTION, Values.KEEP_ALIVE);
          ch.write(response);
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
          response.setHeader(Names.CONNECTION, Values.KEEP_ALIVE);
        }
        // Write the initial line and the header.
        writeFuture = ch.write(response);

        for (FileChunk chunk : file) {
          writeFuture = sendFile(ctx, chunk);
          if (writeFuture == null) {
            sendError(ctx, HttpResponseStatus.NOT_FOUND);
            return;
          }
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
      Channel ch = ctx.getChannel();
      RandomAccessFile spill = null;      
      ChannelFuture writeFuture;
      try {
        spill = new RandomAccessFile(file.getFile(), "r");
        if (ctx.getPipeline().get(SslHandler.class) == null) {
          final FadvisedFileRegion filePart = new FadvisedFileRegion(spill,
              file.startOffset(), file.length(), manageOsCache, readaheadLength,
              readaheadPool, file.getFile().getAbsolutePath());
          writeFuture = ch.write(filePart);
          writeFuture.addListener(new FileCloseListener(filePart));
        } else {
          // HTTPS cannot be done with zero copy.
          final FadvisedChunkedFile chunk = new FadvisedChunkedFile(spill,
              file.startOffset(), file.length(), sslFileBufferSize,
              manageOsCache, readaheadLength, readaheadPool,
              file.getFile().getAbsolutePath());
          writeFuture = ch.write(chunk);
        }
      } catch (FileNotFoundException e) {
        LOG.fatal(file.getFile() + " not found");
        return null;
      } catch (Throwable e) {
        LOG.fatal("error while sending a file: ", e);
        if (spill != null) {
          //should close a opening file
          LOG.warn("Close the file " + file.getFile().getAbsolutePath());
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
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
      response.setHeader(Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
      // Put shuffle version into http header
      ChannelBuffer content = ChannelBuffers.copiedBuffer(message, CharsetUtil.UTF_8);
      response.setContent(content);
      response.setHeader(Names.CONTENT_LENGTH, content.writerIndex());

      // Close the connection as soon as the error message is sent.
      ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {
      Channel ch = e.getChannel();
      Throwable cause = e.getCause();
      LOG.error(cause.getMessage(), cause);
      if (ch.isOpen()) {
        ch.close();
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
