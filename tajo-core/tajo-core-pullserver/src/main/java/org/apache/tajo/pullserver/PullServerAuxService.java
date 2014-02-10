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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
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
import org.apache.tajo.QueryId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.pullserver.retriever.FileChunk;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.util.TajoIdUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.jboss.netty.util.CharsetUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.setContentLength;
import static org.jboss.netty.handler.codec.http.HttpMethod.GET;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class PullServerAuxService extends AuxiliaryService {

  private static final Log LOG = LogFactory.getLog(PullServerAuxService.class);
  
  public static final String SHUFFLE_MANAGE_OS_CACHE = "tajo.pullserver.manage.os.cache";
  public static final boolean DEFAULT_SHUFFLE_MANAGE_OS_CACHE = true;

  public static final String SHUFFLE_READAHEAD_BYTES = "tajo.pullserver.readahead.bytes";
  public static final int DEFAULT_SHUFFLE_READAHEAD_BYTES = 4 * 1024 * 1024;

  private int port;
  private ChannelFactory selector;
  private final ChannelGroup accepted = new DefaultChannelGroup();
  private HttpPipelineFactory pipelineFact;
  private int sslFileBufferSize;

  private ApplicationId appId;
  private QueryId queryId;
  private FileSystem localFS;

  /**
   * Should the shuffle use posix_fadvise calls to manage the OS cache during
   * sendfile
   */
  private boolean manageOsCache;
  private int readaheadLength;
  private ReadaheadPool readaheadPool = ReadaheadPool.getInstance();
   

  public static final String PULLSERVER_SERVICEID = "tajo.pullserver";

  private static final Map<String,String> userRsrc =
    new ConcurrentHashMap<String,String>();
  private static String userName;

  public static final String SUFFLE_SSL_FILE_BUFFER_SIZE_KEY =
    "tajo.pullserver.ssl.file.buffer.size";

  public static final int DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE = 60 * 1024;

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

  PullServerAuxService(MetricsSystem ms) {
    super("httpshuffle");
    metrics = ms.register(new ShuffleMetrics());
  }

  @SuppressWarnings("UnusedDeclaration")
  public PullServerAuxService() {
    this(DefaultMetricsSystem.instance());
  }

  /**
   * Serialize the shuffle port into a ByteBuffer for use later on.
   * @param port the port to be sent to the ApplciationMaster
   * @return the serialized form of the port.
   */
  public static ByteBuffer serializeMetaData(int port) throws IOException {
    //TODO these bytes should be versioned
    DataOutputBuffer port_dob = new DataOutputBuffer();
    port_dob.writeInt(port);
    return ByteBuffer.wrap(port_dob.getData(), 0, port_dob.getLength());
  }

  /**
   * A helper function to deserialize the metadata returned by PullServerAuxService.
   * @param meta the metadata returned by the PullServerAuxService
   * @return the port the PullServer Handler is listening on to serve shuffle data.
   */
  public static int deserializeMetaData(ByteBuffer meta) throws IOException {
    //TODO this should be returning a class not just an int
    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(meta);
    return in.readInt();
  }

  @Override
  public void initializeApplication(ApplicationInitializationContext appInitContext) {
    // TODO these bytes should be versioned
    // TODO: Once SHuffle is out of NM, this can use MR APIs
    this.appId = appInitContext.getApplicationId();
    this.queryId = TajoIdUtils.parseQueryId(appId.toString());
    this.userName = appInitContext.getUser();
    userRsrc.put(this.appId.toString(), this.userName);
  }

  @Override
  public void stopApplication(ApplicationTerminationContext appStopContext) {
    userRsrc.remove(appStopContext.getApplicationId().toString());
  }

  @Override
  public synchronized void init(Configuration conf) {
    try {
      manageOsCache = conf.getBoolean(SHUFFLE_MANAGE_OS_CACHE,
          DEFAULT_SHUFFLE_MANAGE_OS_CACHE);

      readaheadLength = conf.getInt(SHUFFLE_READAHEAD_BYTES,
          DEFAULT_SHUFFLE_READAHEAD_BYTES);

      ThreadFactory bossFactory = new ThreadFactoryBuilder()
          .setNameFormat("PullServerAuxService Netty Boss #%d")
          .build();
      ThreadFactory workerFactory = new ThreadFactoryBuilder()
          .setNameFormat("PullServerAuxService Netty Worker #%d")
          .build();

      selector = new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(bossFactory),
          Executors.newCachedThreadPool(workerFactory));

      localFS = new LocalFileSystem();
      super.init(new Configuration(conf));
    } catch (Throwable t) {
      LOG.error(t);
    }
  }

  // TODO change AbstractService to throw InterruptedException
  @Override
  public synchronized void start() {
    Configuration conf = getConfig();
    ServerBootstrap bootstrap = new ServerBootstrap(selector);
    try {
      pipelineFact = new HttpPipelineFactory(conf);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    bootstrap.setPipelineFactory(pipelineFact);
    port = conf.getInt(ConfVars.PULLSERVER_PORT.varname,
        ConfVars.PULLSERVER_PORT.defaultIntVal);
    Channel ch = bootstrap.bind(new InetSocketAddress(port));
    accepted.add(ch);
    port = ((InetSocketAddress)ch.getLocalAddress()).getPort();
    conf.set(ConfVars.PULLSERVER_PORT.varname, Integer.toString(port));
    pipelineFact.PullServer.setPort(port);
    LOG.info(getName() + " listening on port " + port);
    super.start();

    sslFileBufferSize = conf.getInt(SUFFLE_SSL_FILE_BUFFER_SIZE_KEY,
                                    DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE);
  }

  public int getPort() {
    return port;
  }

  @Override
  public synchronized void stop() {
    try {
      accepted.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
      ServerBootstrap bootstrap = new ServerBootstrap(selector);
      bootstrap.releaseExternalResources();
      pipelineFact.destroy();

      localFS.close();
    } catch (Throwable t) {
      LOG.error(t);
    } finally {
      super.stop();
    }
  }

  @Override
  public synchronized ByteBuffer getMetaData() {
    try {
      return serializeMetaData(port); 
    } catch (IOException e) {
      LOG.error("Error during getMeta", e);
      // TODO add API to AuxiliaryServices to report failures
      return null;
    }
  }

  class HttpPipelineFactory implements ChannelPipelineFactory {

    final PullServer PullServer;
    private SSLFactory sslFactory;

    public HttpPipelineFactory(Configuration conf) throws Exception {
      PullServer = new PullServer(conf);
      if (conf.getBoolean(ConfVars.SHUFFLE_SSL_ENABLED_KEY.varname,
          ConfVars.SHUFFLE_SSL_ENABLED_KEY.defaultBoolVal)) {
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
      pipeline.addLast("decoder", new HttpRequestDecoder());
      pipeline.addLast("aggregator", new HttpChunkAggregator(1 << 16));
      pipeline.addLast("encoder", new HttpResponseEncoder());
      pipeline.addLast("chunking", new ChunkedWriteHandler());
      pipeline.addLast("shuffle", PullServer);
      return pipeline;
      // TODO factor security manager into pipeline
      // TODO factor out encode/decode to permit binary shuffle
      // TODO factor out decode of index to permit alt. models
    }
  }

  class PullServer extends SimpleChannelUpstreamHandler {
    private final Configuration conf;
    private final LocalDirAllocator lDirAlloc = new LocalDirAllocator(ConfVars.WORKER_TEMPORAL_DIR.varname);
    private int port;

    public PullServer(Configuration conf) {
      this.conf = conf;
      this.port = conf.getInt(ConfVars.PULLSERVER_PORT.varname, ConfVars.PULLSERVER_PORT.defaultIntVal);
    }
    
    public void setPort(int port) {
      this.port = port;
    }

    private List<String> splitMaps(List<String> mapq) {
      if (null == mapq) {
        return null;
      }
      final List<String> ret = new ArrayList<String>();
      for (String s : mapq) {
        Collections.addAll(ret, s.split(","));
      }
      return ret;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {

      HttpRequest request = (HttpRequest) e.getMessage();
      if (request.getMethod() != GET) {
        sendError(ctx, METHOD_NOT_ALLOWED);
        return;
      }

      // Parsing the URL into key-values
      final Map<String, List<String>> params =
          new QueryStringDecoder(request.getUri()).getParameters();
      final List<String> types = params.get("type");
      final List<String> taskIdList = params.get("ta");
      final List<String> subQueryIds = params.get("sid");
      final List<String> partitionIds = params.get("p");

      if (types == null || taskIdList == null || subQueryIds == null
          || partitionIds == null) {
        sendError(ctx, "Required type, taskIds, subquery Id, and partition id",
            BAD_REQUEST);
        return;
      }

      if (types.size() != 1 || subQueryIds.size() != 1) {
        sendError(ctx, "Required type, taskIds, subquery Id, and partition id",
            BAD_REQUEST);
        return;
      }

      final List<FileChunk> chunks = Lists.newArrayList();

      String repartitionType = types.get(0);
      String sid = subQueryIds.get(0);
      String partitionId = partitionIds.get(0);
      List<String> taskIds = splitMaps(taskIdList);

      // the working dir of tajo worker for each query
      String queryBaseDir = queryId + "/output" + "/";

      LOG.info("PullServer request param: repartitionType=" + repartitionType +
          ", sid=" + sid + ", partitionId=" + partitionId + ", taskIds=" + taskIdList);

      String taskLocalDir = conf.get(ConfVars.WORKER_TEMPORAL_DIR.varname);
      if (taskLocalDir == null ||
          taskLocalDir.equals("")) {
        LOG.error("Tajo local directory should be specified.");
      }
      LOG.info("PullServer baseDir: " + taskLocalDir + "/" + queryBaseDir);

      // if a subquery requires a range partitioning
      if (repartitionType.equals("r")) {
        String ta = taskIds.get(0);
        Path path = localFS.makeQualified(
            lDirAlloc.getLocalPathToRead(queryBaseDir + "/" + sid + "/"
                + ta + "/output/", conf));

        String startKey = params.get("start").get(0);
        String endKey = params.get("end").get(0);
        boolean last = params.get("final") != null;

        FileChunk chunk;
        try {
          chunk = getFileCunks(path, startKey, endKey, last);
        } catch (Throwable t) {
          LOG.error("ERROR Request: " + request.getUri(), t);
          sendError(ctx, "Cannot get file chunks to be sent", BAD_REQUEST);
          return;
        }
        if (chunk != null) {
          chunks.add(chunk);
        }

        // if a subquery requires a hash repartition
      } else if (repartitionType.equals("h")) {
        for (String ta : taskIds) {
          Path path = localFS.makeQualified(
              lDirAlloc.getLocalPathToRead(queryBaseDir + "/" + sid + "/" +
                  ta + "/output/" + partitionId, conf));
          File file = new File(path.toUri());
          FileChunk chunk = new FileChunk(file, 0, file.length());
          chunks.add(chunk);
        }
      } else {
        LOG.error("Unknown repartition type: " + repartitionType);
        return;
      }

      // Write the content.
      Channel ch = e.getChannel();
      if (chunks.size() == 0) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, NO_CONTENT);
        ch.write(response);
        if (!isKeepAlive(request)) {
          ch.close();
        }
      }  else {
        FileChunk[] file = chunks.toArray(new FileChunk[chunks.size()]);
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        long totalSize = 0;
        for (FileChunk chunk : file) {
          totalSize += chunk.length();
        }
        setContentLength(response, totalSize);

        // Write the initial line and the header.
        ch.write(response);

        ChannelFuture writeFuture = null;

        for (FileChunk chunk : file) {
          writeFuture = sendFile(ctx, ch, chunk);
          if (writeFuture == null) {
            sendError(ctx, NOT_FOUND);
            return;
          }
        }

        // Decide whether to close the connection or not.
        if (!isKeepAlive(request)) {
          // Close the connection when the whole content is written out.
          writeFuture.addListener(ChannelFutureListener.CLOSE);
        }
      }
    }

    private ChannelFuture sendFile(ChannelHandlerContext ctx,
                                   Channel ch,
                                   FileChunk file) throws IOException {
      RandomAccessFile spill;
      try {
        spill = new RandomAccessFile(file.getFile(), "r");
      } catch (FileNotFoundException e) {
        LOG.info(file.getFile() + " not found");
        return null;
      }
      ChannelFuture writeFuture;
      if (ch.getPipeline().get(SslHandler.class) == null) {
        final FadvisedFileRegion partition = new FadvisedFileRegion(spill,
            file.startOffset, file.length(), manageOsCache, readaheadLength,
            readaheadPool, file.getFile().getAbsolutePath());
        writeFuture = ch.write(partition);
        writeFuture.addListener(new ChannelFutureListener() {
          // TODO error handling; distinguish IO/connection failures,
          //      attribute to appropriate spill output
          @Override
          public void operationComplete(ChannelFuture future) {
            partition.releaseExternalResources();
          }
        });
      } else {
        // HTTPS cannot be done with zero copy.
        final FadvisedChunkedFile chunk = new FadvisedChunkedFile(spill,
            file.startOffset, file.length, sslFileBufferSize,
            manageOsCache, readaheadLength, readaheadPool,
            file.getFile().getAbsolutePath());
        writeFuture = ch.write(chunk);
      }
      metrics.shuffleConnections.incr();
      metrics.shuffleOutputBytes.incr(file.length); // optimistic
      return writeFuture;
    }

    private void sendError(ChannelHandlerContext ctx,
        HttpResponseStatus status) {
      sendError(ctx, "", status);
    }

    private void sendError(ChannelHandlerContext ctx, String message,
        HttpResponseStatus status) {
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
      response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
      response.setContent(
        ChannelBuffers.copiedBuffer(message, CharsetUtil.UTF_8));

      // Close the connection as soon as the error message is sent.
      ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {
      Channel ch = e.getChannel();
      Throwable cause = e.getCause();
      if (cause instanceof TooLongFrameException) {
        sendError(ctx, BAD_REQUEST);
        return;
      }

      LOG.error("PullServer error: ", cause);
      if (ch.isConnected()) {
        LOG.error("PullServer error " + e);
        sendError(ctx, INTERNAL_SERVER_ERROR);
      }
    }
  }

  public FileChunk getFileCunks(Path outDir,
                                      String startKey,
                                      String endKey,
                                      boolean last) throws IOException {
    BSTIndex index = new BSTIndex(new TajoConf());
    BSTIndex.BSTIndexReader idxReader =
        index.getIndexReader(new Path(outDir, "index"));
    idxReader.open();
    Schema keySchema = idxReader.getKeySchema();
    TupleComparator comparator = idxReader.getComparator();

    LOG.info("BSTIndex is loaded from disk (" + idxReader.getFirstKey() + ", "
        + idxReader.getLastKey());

    File data = new File(URI.create(outDir.toUri() + "/output"));
    byte [] startBytes = Base64.decodeBase64(startKey);
    byte [] endBytes = Base64.decodeBase64(endKey);

    Tuple start;
    Tuple end;
    try {
      start = RowStoreUtil.RowStoreDecoder.toTuple(keySchema, startBytes);
    } catch (Throwable t) {
      throw new IllegalArgumentException("StartKey: " + startKey
          + ", decoded byte size: " + startBytes.length, t);
    }

    try {
      end = RowStoreUtil.RowStoreDecoder.toTuple(keySchema, endBytes);
    } catch (Throwable t) {
      throw new IllegalArgumentException("EndKey: " + endKey
          + ", decoded byte size: " + endBytes.length, t);
    }


    if(!comparator.isAscendingFirstKey()) {
      Tuple tmpKey = start;
      start = end;
      end = tmpKey;
    }

    LOG.info("GET Request for " + data.getAbsolutePath() + " (start="+start+", end="+ end +
        (last ? ", last=true" : "") + ")");

    if (idxReader.getFirstKey() == null && idxReader.getLastKey() == null) { // if # of rows is zero
      LOG.info("There is no contents");
      return null;
    }

    if (comparator.compare(end, idxReader.getFirstKey()) < 0 ||
        comparator.compare(idxReader.getLastKey(), start) < 0) {
      LOG.info("Out of Scope (indexed data [" + idxReader.getFirstKey() + ", " + idxReader.getLastKey() +
          "], but request start:" + start + ", end: " + end);
      return null;
    }

    long startOffset;
    long endOffset;
    try {
      startOffset = idxReader.find(start);
    } catch (IOException ioe) {
      LOG.error("State Dump (the requested range: "
          + "[" + start + ", " + end +")" + ", idx min: "
          + idxReader.getFirstKey() + ", idx max: "
          + idxReader.getLastKey());
      throw ioe;
    }
    try {
      endOffset = idxReader.find(end);
      if (endOffset == -1) {
        endOffset = idxReader.find(end, true);
      }
    } catch (IOException ioe) {
      LOG.error("State Dump (the requested range: "
          + "[" + start + ", " + end +")" + ", idx min: "
          + idxReader.getFirstKey() + ", idx max: "
          + idxReader.getLastKey());
      throw ioe;
    }

    // if startOffset == -1 then case 2-1 or case 3
    if (startOffset == -1) { // this is a hack
      // if case 2-1 or case 3
      try {
        startOffset = idxReader.find(start, true);
      } catch (IOException ioe) {
        LOG.error("State Dump (the requested range: "
            + "[" + start + ", " + end +")" + ", idx min: "
            + idxReader.getFirstKey() + ", idx max: "
            + idxReader.getLastKey());
        throw ioe;
      }
    }

    if (startOffset == -1) {
      throw new IllegalStateException("startOffset " + startOffset + " is negative \n" +
          "State Dump (the requested range: "
          + "[" + start + ", " + end +")" + ", idx min: " + idxReader.getFirstKey() + ", idx max: "
          + idxReader.getLastKey());
    }

    // if greater than indexed values
    if (last || (endOffset == -1
        && comparator.compare(idxReader.getLastKey(), end) < 0)) {
      endOffset = data.length();
    }

    FileChunk chunk = new FileChunk(data, startOffset, endOffset - startOffset);
    LOG.info("Retrieve File Chunk: " + chunk);
    return chunk;
  }
}
