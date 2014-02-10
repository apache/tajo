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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.worker.dataserver.retriever.FileChunk;
import org.apache.tajo.worker.dataserver.retriever.RetrieverHandler;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 * It retrieves the file chunk ranged between start and end keys.
 * The start key is inclusive, but the end key is exclusive.
 *
 * Internally, there are four cases:
 * <ul>
 *   <li>out of scope: the index range does not overlapped with the query range.</li>
 *   <li>overlapped: the index range is partially overlapped with the query range. </li>
 *   <li>included: the index range is included in the start and end keys</li>
 *   <li>covered: the index range covers the query range (i.e., start and end keys).</li>
 * </ul>
 */
public class RangeRetrieverHandler implements RetrieverHandler {
  private static final Log LOG = LogFactory.getLog(RangeRetrieverHandler.class);
  private final File file;
  private final BSTIndex.BSTIndexReader idxReader;
  private final Schema schema;
  private final TupleComparator comp;

  public RangeRetrieverHandler(File outDir, Schema schema, TupleComparator comp) throws IOException {
    this.file = outDir;
    BSTIndex index = new BSTIndex(new TajoConf());
    this.schema = schema;
    this.comp = comp;
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path indexPath = fs.makeQualified(new Path(outDir.getCanonicalPath(), "index"));
    this.idxReader =
        index.getIndexReader(indexPath, this.schema, this.comp);
    this.idxReader.open();
    LOG.info("BSTIndex is loaded from disk (" + idxReader.getFirstKey() + ", "
        + idxReader.getLastKey());
  }

  @Override
  public FileChunk get(Map<String, List<String>> kvs) throws IOException {
    // nothing to verify the file because AdvancedDataRetriever checks
    // its validity of the file.
    File data = new File(this.file, "data/data");
    byte [] startBytes = Base64.decodeBase64(kvs.get("start").get(0));
    Tuple start = RowStoreUtil.RowStoreDecoder.toTuple(schema, startBytes);
    byte [] endBytes;
    Tuple end;
    endBytes = Base64.decodeBase64(kvs.get("end").get(0));
    end = RowStoreUtil.RowStoreDecoder.toTuple(schema, endBytes);
    boolean last = kvs.containsKey("final");

    if(!comp.isAscendingFirstKey()) {
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

    if (comp.compare(end, idxReader.getFirstKey()) < 0 ||
        comp.compare(idxReader.getLastKey(), start) < 0) {
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
          + "[" + start + ", " + end+")" + ", idx min: " + idxReader.getFirstKey() + ", idx max: "
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
          + "[" + start + ", " + end+")" + ", idx min: " + idxReader.getFirstKey() + ", idx max: "
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
            + "[" + start + ", " + end+")" + ", idx min: " + idxReader.getFirstKey() + ", idx max: "
            + idxReader.getLastKey());
        throw ioe;
      }
    }

    if (startOffset == -1) {
      throw new IllegalStateException("startOffset " + startOffset + " is negative \n" +
          "State Dump (the requested range: "
          + "[" + start + ", " + end+")" + ", idx min: " + idxReader.getFirstKey() + ", idx max: "
          + idxReader.getLastKey());
    }

    // if greater than indexed values
    if (last || (endOffset == -1 && comp.compare(idxReader.getLastKey(), end) < 0)) {
      endOffset = data.length();
    }

    FileChunk chunk = new FileChunk(data, startOffset, endOffset - startOffset);
    LOG.info("Retrieve File Chunk: " + chunk);
    return chunk;
  }
}
