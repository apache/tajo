/*
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

package org.apache.tajo.storage.http;

import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.storage.EmptyTuple;
import org.apache.tajo.storage.FileScanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.json.JsonLineDeserializer;
import org.apache.tajo.storage.text.TextLineParsingError;
import org.apache.tajo.unit.StorageUnit;

import java.io.IOException;

import static org.apache.tajo.storage.StorageConstants.DEFAULT_TEXT_ERROR_TOLERANCE_MAXNUM;
import static org.apache.tajo.storage.StorageConstants.TEXT_ERROR_TOLERANCE_MAXNUM;
import static org.apache.tajo.storage.text.DelimitedTextFile.READ_BUFFER_SIZE;

public class ExampleHttpJsonScanner extends FileScanner {

  private static final Log LOG = LogFactory.getLog(ExampleHttpJsonScanner.class);

  private VTuple outTuple;

  private long limit;

  private final long startOffset;
  private final long endOffset;

  private ExampleHttpJsonLineReader reader;
  private JsonLineDeserializer deserializer;

  private int errorPrintOutMaxNum = 5;
  /** Maximum number of permissible errors */
  private final int maxAllowedErrorCount;
  /** How many errors have occurred? */
  private int errorNum;

  private boolean splittable = false;

  private long recordCount = 0;

  public ExampleHttpJsonScanner(Configuration conf, Schema schema, TableMeta tableMeta, Fragment fragment)
      throws IOException {
    super(conf, schema, tableMeta, fragment);

    reader = new ExampleHttpJsonLineReader(conf, this.fragment, conf.getInt(READ_BUFFER_SIZE, 128 * StorageUnit.KB));
    if (!this.reader.isCompressed()) {
      splittable = true;
    }

    startOffset = this.fragment.getStartKey();
    endOffset = this.fragment.getEndKey();

    maxAllowedErrorCount =
        Integer.parseInt(tableMeta.getProperty(TEXT_ERROR_TOLERANCE_MAXNUM, DEFAULT_TEXT_ERROR_TOLERANCE_MAXNUM));
  }

  @Override
  public void init() throws IOException {

    reader.init();

    if (targets == null) {
      targets = schema.toArray();
    }

    reset();

    super.init();
  }

  @Override
  public Tuple next() throws IOException {

    if (reader.isEof()) {
      return null; // Indicate to the parent operator that there is no more data.
    }

    // Read lines until it reads a valid tuple or EOS (end of stream).
    while (!reader.isEof()) {

      ByteBuf buf = reader.readLine();

      if (buf == null) { // The null buf means that there is no more lines.
        return null;
      }

      // When the number of projection columns is 0, the read line doesn't have to be parsed.
      if (targets.length == 0) {
        recordCount++;
        return EmptyTuple.get();
      }

      try {
        deserializer.deserialize(buf, outTuple);

        // Once a line is normally parsed, exits the while loop.
        break;

      } catch (TextLineParsingError tlpe) {

        errorNum++;

        // The below line may print too many logs.
        LOG.warn("Ignore Text Parse Error (" + errorNum + "): ", tlpe);

        // If the number of found errors exceeds the configured tolerable error count,
        // throw the error.
        if (maxAllowedErrorCount >= 0 && errorNum > maxAllowedErrorCount) {
          throw new IOException(tlpe);
        }
      }
    }

    recordCount++;

    return outTuple;
  }

  @Override
  public void reset() throws IOException {
    recordCount = 0;

    if (reader.getReadBytes() > 0) {
      reader.close();

      reader = new ExampleHttpJsonLineReader(conf, fragment, conf.getInt(READ_BUFFER_SIZE, 128 * StorageUnit.KB));
      reader.init();
    }

    if(deserializer != null) {
      deserializer.release();
    }

    deserializer = new JsonLineDeserializer(schema, meta, targets);
    deserializer.init();

    outTuple = new VTuple(targets.length);

    // skip first line if it reads from middle of file
    if (startOffset > 0) {
      reader.readLine();
    }
  }

  @Override
  public void close() throws IOException {
    try {

      if (deserializer != null) {
        deserializer.release();
      }

      if (reader != null) {
        inputStats.setReadBytes(reader.getReadBytes());
        inputStats.setNumRows(recordCount);
      }

    } finally {
      IOUtils.cleanup(LOG, reader);
      outTuple = null;
    }
  }

  @Override
  public boolean isProjectable() {
    return true;
  }

  @Override
  public void setTarget(Column[] targets) {
    this.targets = targets;
  }

  @Override
  public boolean isSelectable() {
    return false;
  }

  @Override
  public void setFilter(EvalNode filter) {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public void setLimit(long num) {
    this.limit = num;
  }

  @Override
  public boolean isSplittable() {
    return splittable;
  }

  @Override
  public float getProgress() {
    if(!inited) return super.getProgress();

    if (reader.isEof()) { // if the reader reaches EOS
      return 1.0f;
    }

    long currentPos = reader.getPos();
    long readBytes = currentPos - startOffset;
    long remainingBytes = Math.max(endOffset - currentPos, 0);
    return Math.min(1.0f, (float) (readBytes) / (float) (readBytes + remainingBytes));
  }

  @Override
  public TableStats getInputStats() {
    if (inputStats != null && reader != null) {
      inputStats.setReadBytes(reader.getReadBytes());  //Actual Processed Bytes. (decompressed bytes + overhead)
      inputStats.setNumRows(recordCount);
      inputStats.setNumBytes(fragment.getLength());
    }

    return inputStats;
  }
}
