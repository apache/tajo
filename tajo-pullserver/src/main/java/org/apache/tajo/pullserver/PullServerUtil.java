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

import com.google.common.base.Preconditions;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.pullserver.PullServerConstants.Param;
import org.apache.tajo.pullserver.retriever.FileChunk;
import org.apache.tajo.pullserver.retriever.FileChunkMeta;
import org.apache.tajo.pullserver.retriever.IndexCacheKey;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.RowStoreUtil.RowStoreDecoder;
import org.apache.tajo.storage.index.bst.BSTIndex.BSTIndexReader;
import org.apache.tajo.util.Pair;

import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PullServerUtil {
  private static final Log LOG = LogFactory.getLog(PullServerUtil.class);

  private static boolean nativeIOPossible = false;

  static {
    if (NativeIO.isAvailable()) {
      nativeIOPossible = true;
    } else {
      LOG.warn("Unable to load hadoop nativeIO");
    }
  }

  public static boolean isNativeIOPossible() {
    return nativeIOPossible;
  }

  /**
   * Call posix_fadvise on the given file descriptor. See the manpage
   * for this syscall for more information. On systems where this
   * call is not available, does nothing.
   */
  public static void posixFadviseIfPossible(String identifier, java.io.FileDescriptor fd,
                                            long offset, long len, int flags) {
    if (nativeIOPossible) {
      try {
        NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(identifier, fd, offset, len, flags);
      } catch (Throwable t) {
        nativeIOPossible = false;
        LOG.warn("Failed to manage OS cache for " + identifier, t);
      }
    }
  }

  public static Path getBaseOutputDir(String queryId, String executionBlockSequenceId) {
    return StorageUtil.concatPath(
            queryId,
            "output",
            executionBlockSequenceId);
  }

  public static Path getBaseInputDir(String queryId, String executionBlockId) {
    return StorageUtil.concatPath(
            queryId,
            "in",
            executionBlockId);
  }

  public static List<String> splitMaps(List<String> mapq) {
    if (null == mapq) {
      return null;
    }
    final List<String> ret = new ArrayList<>();
    for (String s : mapq) {
      Collections.addAll(ret, s.split(","));
    }
    return ret;
  }


  public static boolean isChunkRequest(String requestType) {
    return requestType.equals(PullServerConstants.CHUNK_REQUEST_PARAM_STRING);
  }

  public static boolean isMetaRequest(String requestType) {
    return requestType.equals(PullServerConstants.META_REQUEST_PARAM_STRING);
  }

  public static boolean isRangeShuffle(String shuffleType) {
    return shuffleType.equals(PullServerConstants.RANGE_SHUFFLE_PARAM_STRING);
  }

  public static boolean isHashShuffle(String shuffleType) {
    return shuffleType.equals(PullServerConstants.HASH_SHUFFLE_PARAM_STRING)
        || shuffleType.equals(PullServerConstants.SCATTERED_HASH_SHUFFLE_PARAM_STRING);
  }

  public static class PullServerParams extends HashMap<String, List<String>> {

    public PullServerParams(URI uri) {
      this(uri.toString());
    }

    public PullServerParams(String uri) {
      super(new QueryStringDecoder(uri).parameters());
    }

    public boolean contains(Param param) {
      return containsKey(param.key());
    }

    public List<String> get(Param param) {
      return get(param.key());
    }

    private String checkAndGetFirstParam(Param param) {
      Preconditions.checkArgument(contains(param), "Missing " + param.name());
      Preconditions.checkArgument(get(param).size() == 1, "Too many params: " + param.name());
      return get(param).get(0);
    }

    private List<String> checkAndGet(Param param) {
      Preconditions.checkArgument(contains(param), "Missing " + param.name());
      return get(param);
    }

    public String requestType() {
      return checkAndGetFirstParam(Param.REQUEST_TYPE);
    }

    public String shuffleType() {
      return checkAndGetFirstParam(Param.SHUFFLE_TYPE);
    }

    public String queryId() {
      return checkAndGetFirstParam(Param.QUERY_ID);
    }

    public String ebId() {
      return checkAndGetFirstParam(Param.EB_ID);
    }

    public long offset() {
      return contains(Param.OFFSET) && get(Param.OFFSET).size() == 1 ?
          Long.parseLong(get(Param.OFFSET).get(0)) : -1L;
    }

    public long length() {
      return contains(Param.LENGTH) && get(Param.LENGTH).size() == 1 ?
          Long.parseLong(get(Param.LENGTH).get(0)) : -1L;
    }

    public String startKey() {
      return checkAndGetFirstParam(Param.START);
    }

    public String endKey() {
      return checkAndGetFirstParam(Param.END);
    }

    public boolean last() {
      return contains(Param.FINAL);
    }

    public String partId() {
      return checkAndGetFirstParam(Param.PART_ID);
    }

    public List<String> taskAttemptIds() {
      return checkAndGet(Param.TASK_ID);
    }
  }

  public static class PullServerRequestURIBuilder {
    private final StringBuilder builder = new StringBuilder("http://");
    private String requestType;
    private String shuffleType;
    private String queryId;
    private Integer ebId;
    private Integer partId;
    private List<Integer> taskIds;
    private List<Integer> attemptIds;
    private List<String> taskAttemptIds;
    private Long offset;
    private Long length;
    private String startKeyBase64;
    private String endKeyBase64;
    private boolean last;
    private final int maxUrlLength;

    public PullServerRequestURIBuilder(String pullServerAddr, int pullServerPort, int maxUrlLength) {
      this(pullServerAddr, Integer.toString(pullServerPort), maxUrlLength);
    }

    public PullServerRequestURIBuilder(String pullServerAddr, String pullServerPort, int maxUrlLength) {
      builder.append(pullServerAddr).append(":").append(pullServerPort).append("/?");
      this.maxUrlLength = maxUrlLength;
    }

    public List<URI> build(boolean includeTasks) {
      append(Param.REQUEST_TYPE, requestType)
          .append(Param.QUERY_ID, queryId)
          .append(Param.EB_ID, ebId)
          .append(Param.PART_ID, partId)
          .append(Param.SHUFFLE_TYPE, shuffleType);

      if (startKeyBase64 != null) {

        try {
          append(Param.START, URLEncoder.encode(startKeyBase64, "utf-8"))
              .append(Param.END, URLEncoder.encode(endKeyBase64, "utf-8"));
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException(e);
        }

        if (last) {
          append(Param.FINAL, Boolean.toString(last));
        }
      }

      if (length != null) {
        append(Param.OFFSET, offset.toString())
            .append(Param.LENGTH, length.toString());
      }

      List<URI> results = new ArrayList<>();
      if (!includeTasks || isHashShuffle(shuffleType)) {
        results.add(URI.create(builder.toString()));
      } else {
        builder.append(Param.TASK_ID.key()).append("=");
        List<String> taskAttemptIds = this.taskAttemptIds;
        if (taskAttemptIds == null) {

          // Sort task ids to increase cache hit in pull server
          taskAttemptIds = IntStream.range(0, taskIds.size())
              .mapToObj(i -> new Pair<>(taskIds.get(i), attemptIds.get(i)))
              .sorted((p1, p2) -> p1.getFirst() - p2.getFirst())
              // In the case of hash shuffle each partition has single shuffle file per worker.
              // TODO If file is large, consider multiple fetching(shuffle file can be split)
              .filter(pair -> pair.getFirst() >= 0)
              .map(pair -> pair.getFirst() + "_" + pair.getSecond())
              .collect(Collectors.toList());
        }

        // If the get request is longer than 2000 characters,
        // the long request uri may cause HTTP Status Code - 414 Request-URI Too Long.
        // Refer to http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.4.15
        // The below code transforms a long request to multiple requests.
        List<String> taskIdsParams = new ArrayList<>();
        StringBuilder taskIdListBuilder = new StringBuilder();

        boolean first = true;
        for (int i = 0; i < taskAttemptIds.size(); i++) {
          if (!first) {
            taskIdListBuilder.append(",");
          }
          first = false;

          if (builder.length() + taskIdListBuilder.length() > maxUrlLength) {
            taskIdsParams.add(taskIdListBuilder.toString());
            taskIdListBuilder = new StringBuilder(taskAttemptIds.get(i));
          } else {
            taskIdListBuilder.append(taskAttemptIds.get(i));
          }
        }
        // if the url params remain
        if (taskIdListBuilder.length() > 0) {
          taskIdsParams.add(taskIdListBuilder.toString());
        }
        for (String param : taskIdsParams) {
          results.add(URI.create(builder + param));
        }
      }

      return results;
    }

    private PullServerRequestURIBuilder append(Param key, Object val) {
      builder.append(key.key())
          .append("=")
          .append(val)
          .append("&");

      return this;
    }

    public PullServerRequestURIBuilder setRequestType(String type) {
      this.requestType = type;
      return this;
    }

    public PullServerRequestURIBuilder setShuffleType(String shuffleType) {
      this.shuffleType = shuffleType;
      return this;
    }

    public PullServerRequestURIBuilder setQueryId(String queryId) {
      this.queryId = queryId;
      return this;
    }

    public PullServerRequestURIBuilder setEbId(String ebId) {
      this.ebId = Integer.parseInt(ebId);
      return this;
    }

    public PullServerRequestURIBuilder setEbId(Integer ebId) {
      this.ebId = ebId;
      return this;
    }

    public PullServerRequestURIBuilder setPartId(String partId) {
      this.partId = Integer.parseInt(partId);
      return this;
    }

    public PullServerRequestURIBuilder setPartId(Integer partId) {
      this.partId = partId;
      return this;
    }

    public PullServerRequestURIBuilder setTaskIds(List<Integer> taskIds) {
      this.taskIds = taskIds;
      return this;
    }

    public PullServerRequestURIBuilder setAttemptIds(List<Integer> attemptIds) {
      this.attemptIds = attemptIds;
      return this;
    }

    public PullServerRequestURIBuilder setTaskAttemptIds(List<String> taskAttemptIds) {
      this.taskAttemptIds = taskAttemptIds;
      return this;
    }

    public PullServerRequestURIBuilder setOffset(long offset) {
      this.offset = offset;
      return this;
    }

    public PullServerRequestURIBuilder setLength(long length) {
      this.length = length;
      return this;
    }

    public PullServerRequestURIBuilder setStartKeyBase64(String startKeyBase64) {
      this.startKeyBase64 = startKeyBase64;
      return this;
    }

    public PullServerRequestURIBuilder setEndKeyBase64(String endKeyBase64) {
      this.endKeyBase64 = endKeyBase64;
      return this;
    }

    public PullServerRequestURIBuilder setLastInclude(boolean last) {
      this.last = last;
      return this;
    }
  }

  public static boolean useExternalPullServerService(TajoConf conf) {
    // TODO: add more service types like mesos
    return TajoPullServerService.isStandalone()
        || conf.getBoolVar(ConfVars.YARN_SHUFFLE_SERVICE_ENABLED);
  }

  private static FileChunkMeta searchFileChunkMeta(String queryId,
                                                  String ebSeqId,
                                                  String taskId,
                                                  Path outDir,
                                                  String startKey,
                                                  String endKey,
                                                  boolean last,
                                                  LoadingCache<IndexCacheKey, BSTIndexReader> indexReaderCache,
                                                  int lowCacheHitCheckThreshold) throws IOException, ExecutionException {
    SearchResult result = searchCorrespondPart(queryId, ebSeqId, outDir, startKey, endKey, last,
        indexReaderCache, lowCacheHitCheckThreshold);
    // Do not send file chunks of 0 length
    if (result != null) {
      long startOffset = result.startOffset;
      long endOffset = result.endOffset;

      FileChunkMeta chunk = new FileChunkMeta(startOffset, endOffset - startOffset, ebSeqId, taskId);

      if (LOG.isDebugEnabled()) LOG.debug("Retrieve File Chunk: " + chunk);
      return chunk;
    } else {
      return null;
    }
  }

  private static FileChunk searchFileChunk(String queryId,
                                           String ebSeqId,
                                           Path outDir,
                                           String startKey,
                                           String endKey,
                                           boolean last,
                                           LoadingCache<IndexCacheKey, BSTIndexReader> indexReaderCache,
                                           int lowCacheHitCheckThreshold) throws IOException, ExecutionException {

    final SearchResult result = searchCorrespondPart(queryId, ebSeqId, outDir, startKey, endKey, last,
        indexReaderCache, lowCacheHitCheckThreshold);
    if (result != null) {
      long startOffset = result.startOffset;
      long endOffset = result.endOffset;
      FileChunk chunk = new FileChunk(result.data, startOffset, endOffset - startOffset);

      if (LOG.isDebugEnabled()) LOG.debug("Retrieve File Chunk: " + chunk);
      return chunk;
    } else {
      return null;
    }
  }

  private static class SearchResult {
    File data;
    long startOffset;
    long endOffset;

    public SearchResult(File data, long startOffset, long endOffset) {
      this.data = data;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }
  }

  private static SearchResult searchCorrespondPart(String queryId,
                                                   String ebSeqId,
                                                   Path outDir,
                                                   String startKey,
                                                   String endKey,
                                                   boolean last,
                                                   LoadingCache<IndexCacheKey, BSTIndexReader> indexReaderCache,
                                                   int lowCacheHitCheckThreshold) throws IOException, ExecutionException {
    BSTIndexReader idxReader = indexReaderCache.get(new IndexCacheKey(outDir, queryId, ebSeqId));
    idxReader.retain();

    File data;
    long startOffset;
    long endOffset;
    try {
      if (LOG.isDebugEnabled()) {
        if (indexReaderCache.size() > lowCacheHitCheckThreshold && indexReaderCache.stats().hitRate() < 0.5) {
          LOG.debug("Too low cache hit rate: " + indexReaderCache.stats());
        }
      }

      Tuple indexedFirst = idxReader.getFirstKey();
      Tuple indexedLast = idxReader.getLastKey();

      if (indexedFirst == null && indexedLast == null) { // if # of rows is zero
        if (LOG.isDebugEnabled()) {
          LOG.debug("There is no contents");
        }
        return null;
      }

      byte[] startBytes = Base64.decodeBase64(startKey);
      byte[] endBytes = Base64.decodeBase64(endKey);


      Tuple start;
      Tuple end;
      Schema keySchema = idxReader.getKeySchema();
      RowStoreDecoder decoder = RowStoreUtil.createDecoder(keySchema);

      try {
        start = decoder.toTuple(startBytes);
      } catch (Throwable t) {
        throw new IllegalArgumentException("StartKey: " + startKey
            + ", decoded byte size: " + startBytes.length, t);
      }

      try {
        end = decoder.toTuple(endBytes);
      } catch (Throwable t) {
        throw new IllegalArgumentException("EndKey: " + endKey
            + ", decoded byte size: " + endBytes.length, t);
      }

      data = new File(URI.create(outDir.toUri() + "/output"));
      if (LOG.isDebugEnabled()) {
        LOG.debug("GET Request for " + data.getAbsolutePath() + " (start=" + start + ", end=" + end +
            (last ? ", last=true" : "") + ")");
      }

      TupleComparator comparator = idxReader.getComparator();

      if (comparator.compare(end, indexedFirst) < 0 ||
          comparator.compare(indexedLast, start) < 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Out of Scope (indexed data [" + indexedFirst + ", " + indexedLast +
              "], but request start:" + start + ", end: " + end);
        }
        return null;
      }

      try {
        idxReader.init();
        startOffset = idxReader.find(start);
      } catch (IOException ioe) {
        LOG.error("State Dump (the requested range: "
            + "[" + start + ", " + end + ")" + ", idx min: "
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
            + "[" + start + ", " + end + ")" + ", idx min: "
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
              + "[" + start + ", " + end + ")" + ", idx min: "
              + idxReader.getFirstKey() + ", idx max: "
              + idxReader.getLastKey());
          throw ioe;
        }
      }

      if (startOffset == -1) {
        throw new IllegalStateException("startOffset " + startOffset + " is negative \n" +
            "State Dump (the requested range: "
            + "[" + start + ", " + end + ")" + ", idx min: " + idxReader.getFirstKey() + ", idx max: "
            + idxReader.getLastKey());
      }

      // if greater than indexed values
      if (last || (endOffset == -1
          && comparator.compare(idxReader.getLastKey(), end) < 0)) {
        endOffset = data.length();
      }
    } finally {
      idxReader.release();
    }

    return new SearchResult(data, startOffset, endOffset);
  }

  /**
   * Retrieve meta information of file chunks which correspond to the requested URI.
   * Only meta information for the file chunks which has non-zero length are retrieved.
   *
   * @param conf
   * @param lDirAlloc
   * @param localFS
   * @param params
   * @param gson
   * @param indexReaderCache
   * @param lowCacheHitCheckThreshold
   * @return
   * @throws IOException
   * @throws ExecutionException
   */
  public static List<String> getJsonMeta(final TajoConf conf,
                                         final LocalDirAllocator lDirAlloc,
                                         final FileSystem localFS,
                                         final PullServerParams params,
                                         final Gson gson,
                                         final LoadingCache<IndexCacheKey, BSTIndexReader> indexReaderCache,
                                         final int lowCacheHitCheckThreshold)
      throws IOException, ExecutionException {
    final List<String> taskIds = PullServerUtil.splitMaps(params.taskAttemptIds());
    final Path queryBaseDir = PullServerUtil.getBaseOutputDir(params.queryId(), params.ebId());
    final List<String> jsonMetas = new ArrayList<>();

    for (String eachTaskId : taskIds) {
      Path outputPath = StorageUtil.concatPath(queryBaseDir, eachTaskId, "output");
      if (!lDirAlloc.ifExists(outputPath.toString(), conf)) {
        LOG.warn("Range shuffle - file not exist. " + outputPath);
        continue;
      }
      Path path = localFS.makeQualified(lDirAlloc.getLocalPathToRead(outputPath.toString(), conf));
      FileChunkMeta meta;
      meta = PullServerUtil.searchFileChunkMeta(params.queryId(), params.ebId(), eachTaskId, path,
          params.startKey(), params.endKey(), params.last(), indexReaderCache, lowCacheHitCheckThreshold);
      if (meta != null && meta.getLength() > 0) {
        String jsonStr = gson.toJson(meta, FileChunkMeta.class);
        jsonMetas.add(jsonStr);
      }
    }
    return jsonMetas;
  }

  /**
   * Retrieve file chunks which correspond to the requested URI.
   * Only the file chunks which has non-zero length are retrieved.
   *
   * @param conf
   * @param lDirAlloc
   * @param localFS
   * @param params
   * @param indexReaderCache
   * @param lowCacheHitCheckThreshold
   * @return
   * @throws IOException
   * @throws ExecutionException
   */
  public static List<FileChunk> getFileChunks(final TajoConf conf,
                                              final LocalDirAllocator lDirAlloc,
                                              final FileSystem localFS,
                                              final PullServerParams params,
                                              final LoadingCache<IndexCacheKey, BSTIndexReader> indexReaderCache,
                                              final int lowCacheHitCheckThreshold)
      throws IOException, ExecutionException {
    final List<FileChunk> chunks = new ArrayList<>();

    final String queryId = params.queryId();
    final String shuffleType = params.shuffleType();
    final String sid =  params.ebId();

    final long offset = params.offset();
    final long length = params.length();

    final Path queryBaseDir = PullServerUtil.getBaseOutputDir(queryId, sid);

    if (LOG.isDebugEnabled()) {
      LOG.debug("PullServer request param: shuffleType=" + shuffleType + ", sid=" + sid);

      // the working dir of tajo worker for each query
      LOG.debug("PullServer baseDir: " + conf.get(ConfVars.WORKER_TEMPORAL_DIR.varname) + "/" + queryBaseDir);
    }

    // if a stage requires a range shuffle
    if (PullServerUtil.isRangeShuffle(shuffleType)) {
      final List<String> taskIdList = params.taskAttemptIds();
      final List<String> taskIds = PullServerUtil.splitMaps(taskIdList);

      final String startKey = params.startKey();
      final String endKey = params.endKey();
      final boolean last = params.last();

      long before = System.currentTimeMillis();
      for (String eachTaskId : taskIds) {
        Path outputPath = StorageUtil.concatPath(queryBaseDir, eachTaskId, "output");
        if (!lDirAlloc.ifExists(outputPath.toString(), conf)) {
          LOG.warn(outputPath + " does not exist.");
          continue;
        }
        Path path = localFS.makeQualified(lDirAlloc.getLocalPathToRead(outputPath.toString(), conf));

        FileChunk chunk = PullServerUtil.searchFileChunk(queryId, sid, path, startKey, endKey, last, indexReaderCache,
            lowCacheHitCheckThreshold);
        if (chunk != null) {
          chunks.add(chunk);
        }
      }
      long after = System.currentTimeMillis();
      LOG.info("Index lookup time: " + (after - before) + " ms");

      // if a stage requires a hash shuffle or a scattered hash shuffle
    } else if (PullServerUtil.isHashShuffle(shuffleType)) {

      final String partId = params.partId();
      int partParentId = HashShuffleAppenderManager.getPartParentId(Integer.parseInt(partId), conf);
      Path partPath = StorageUtil.concatPath(queryBaseDir, "hash-shuffle", String.valueOf(partParentId), partId);
      if (!lDirAlloc.ifExists(partPath.toString(), conf)) {
        throw new FileNotFoundException(partPath.toString());
      }

      Path path = localFS.makeQualified(lDirAlloc.getLocalPathToRead(partPath.toString(), conf));

      File file = new File(path.toUri());
      long startPos = (offset >= 0 && length >= 0) ? offset : 0;
      long readLen = (offset >= 0 && length >= 0) ? length : file.length();

      if (startPos >= file.length()) {
        String errorMessage = "Start pos[" + startPos + "] great than file length [" + file.length() + "]";
        throw new EOFException(errorMessage);
      }
      FileChunk chunk = new FileChunk(file, startPos, readLen);
      chunks.add(chunk);
    } else {
      throw new IllegalArgumentException(shuffleType);
    }
    return chunks.stream().filter(c -> c.length() > 0).collect(Collectors.toList());
  }
}
