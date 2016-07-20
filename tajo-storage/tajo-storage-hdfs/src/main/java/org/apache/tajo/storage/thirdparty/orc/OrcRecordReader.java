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

package org.apache.tajo.storage.thirdparty.orc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.orc.*;
import org.apache.orc.impl.*;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.fragment.AbstractFileFragment;
import org.apache.tajo.storage.thirdparty.orc.TreeReaderFactory.DatumTreeReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

public class OrcRecordReader implements Closeable {

  private static final Log LOG = LogFactory.getLog(OrcRecordReader.class);

  private final Path path;
  private final long firstRow;
  private final List<StripeInformation> stripes = new ArrayList<>();
  private OrcProto.StripeFooter stripeFooter;
  private final long totalRowCount;
  private final CompressionCodec codec;
  private final List<OrcProto.Type> types;
  private final int bufferSize;
  private final boolean[] included;
  private final long rowIndexStride;
  private long rowInStripe = 0;
  private int currentStripe = -1;
  private long rowBaseInStripe = 0;
  private long rowCountInStripe = 0;
  private final Map<org.apache.orc.impl.StreamName, InStream> streams = new HashMap<>();
  DiskRangeList bufferChunks = null;
  private final TreeReaderFactory.DatumTreeReader[] reader;
  private final OrcProto.RowIndex[] indexes;
  private final OrcProto.BloomFilterIndex[] bloomFilterIndices;
  private final Configuration conf;
  private final MetadataReader metadata;
  private final DataReader dataReader;
  private final Tuple result;

  public OrcRecordReader(List<StripeInformation> stripes,
                         FileSystem fileSystem,
                         Schema schema,
                         Column[] targets,
                         AbstractFileFragment fragment,
                         List<OrcProto.Type> types,
                         CompressionCodec codec,
                         int bufferSize,
                         long strideRate,
                         Reader.Options options,
                         Configuration conf,
                         TimeZone timeZone) throws IOException {

    result = new VTuple(targets.length);

    this.conf = conf;
    this.path = fragment.getPath();
    this.codec = codec;
    this.types = types;
    this.bufferSize = bufferSize;
    this.included = new boolean[schema.size() + 1];
    included[0] = targets.length > 0; // always include root column except when target schema size is 0
    Schema targetSchema = SchemaBuilder.builder().addAll(targets).build();
    for (int i = 1; i < included.length; i++) {
      included[i] = targetSchema.contains(schema.getColumn(i - 1));
    }
    this.rowIndexStride = strideRate;
    this.metadata = new MetadataReaderImpl(fileSystem, path, codec, bufferSize, types.size());

    long rows = 0;
    long skippedRows = 0;
    long offset = fragment.getStartKey();
    long maxOffset = fragment.getEndKey();
    for(StripeInformation stripe: stripes) {
      long stripeStart = stripe.getOffset();
      if (offset > stripeStart) {
        skippedRows += stripe.getNumberOfRows();
      } else if (stripeStart < maxOffset) {
        this.stripes.add(stripe);
        rows += stripe.getNumberOfRows();
      }
    }

    // TODO: we could change the ctor to pass this externally
    this.dataReader = RecordReaderUtils.createDefaultDataReader(fileSystem, path, options.getUseZeroCopy(), codec);
    this.dataReader.open();

    firstRow = skippedRows;
    totalRowCount = rows;

    reader = new DatumTreeReader[targets.length];
    for (int i = 0; i < reader.length; i++) {
      reader[i] = TreeReaderFactory.createTreeReader(timeZone, schema.getColumnId(targets[i].getQualifiedName()), targets[i],
          options.getSkipCorruptRecords());
    }

    indexes = new OrcProto.RowIndex[types.size()];
    bloomFilterIndices = new OrcProto.BloomFilterIndex[types.size()];
    advanceToNextRow(reader, 0L, true);
  }

  /**
   * Plan the ranges of the file that we need to read given the list of
   * columns and row groups.
   *
   * @param streamList        the list of streams available
   * @param includedColumns   which columns are needed
   * @param doMergeBuffers
   * @return the list of disk ranges that will be loaded
   */
  static DiskRangeList planReadPartialDataStreams
  (List<OrcProto.Stream> streamList,
   boolean[] includedColumns,
   boolean doMergeBuffers) {
    long offset = 0;
    // figure out which columns have a present stream
    DiskRangeList.CreateHelper list = new DiskRangeList.CreateHelper();
    for (OrcProto.Stream stream : streamList) {
      long length = stream.getLength();
      int column = stream.getColumn();
      OrcProto.Stream.Kind streamKind = stream.getKind();
      // since stream kind is optional, first check if it exists
      if (stream.hasKind() &&
          (org.apache.orc.impl.StreamName.getArea(streamKind) == org.apache.orc.impl.StreamName.Area.DATA) &&
          includedColumns[column]) {
        RecordReaderUtils.addEntireStreamToRanges(offset, length, list, doMergeBuffers);
      }
      offset += length;
    }
    return list.extract();
  }

  void createStreams(List<OrcProto.Stream> streamDescriptions,
                     DiskRangeList ranges,
                     boolean[] includeColumn,
                     CompressionCodec codec,
                     int bufferSize,
                     Map<org.apache.orc.impl.StreamName, InStream> streams) throws IOException {
    long streamOffset = 0;
    for (OrcProto.Stream streamDesc : streamDescriptions) {
      int column = streamDesc.getColumn();
      if ((includeColumn != null && !includeColumn[column]) ||
          streamDesc.hasKind() &&
              (org.apache.orc.impl.StreamName.getArea(streamDesc.getKind()) != org.apache.orc.impl.StreamName.Area.DATA)) {
        streamOffset += streamDesc.getLength();
        continue;
      }
      List<DiskRange> buffers = RecordReaderUtils.getStreamBuffers(
          ranges, streamOffset, streamDesc.getLength());
      org.apache.orc.impl.StreamName name = new StreamName(column, streamDesc.getKind());
      streams.put(name, InStream.create(name.toString(), buffers,
          streamDesc.getLength(), codec, bufferSize));
      streamOffset += streamDesc.getLength();
    }
  }

  private void readPartialDataStreams(StripeInformation stripe) throws IOException {
    List<OrcProto.Stream> streamList = stripeFooter.getStreamsList();
    DiskRangeList toRead = planReadPartialDataStreams(streamList, included, true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("chunks = " + RecordReaderUtils.stringifyDiskRanges(toRead));
    }
    bufferChunks = dataReader.readFileData(toRead, stripe.getOffset(), false);
    if (LOG.isDebugEnabled()) {
      LOG.debug("merge = " + RecordReaderUtils.stringifyDiskRanges(bufferChunks));
    }

    createStreams(streamList, bufferChunks, included, codec, bufferSize, streams);
  }

  /**
   * Skip over rows that we aren't selecting, so that the next row is
   * one that we will read.
   *
   * @param nextRow the row we want to go to
   * @throws IOException
   */
  private boolean advanceToNextRow(
      TreeReaderFactory.TreeReader[] reader, long nextRow, boolean canAdvanceStripe)
      throws IOException {
    long nextRowInStripe = nextRow - rowBaseInStripe;

    if (nextRowInStripe >= rowCountInStripe) {
      if (canAdvanceStripe) {
        advanceStripe();
      }
      return canAdvanceStripe;
    }
    if (nextRowInStripe != rowInStripe) {
      if (rowIndexStride != 0) {
        int rowGroup = (int) (nextRowInStripe / rowIndexStride);
        seekToRowEntry(reader, rowGroup);
        for (TreeReaderFactory.TreeReader eachReader : reader) {
          eachReader.skipRows(nextRowInStripe - rowGroup * rowIndexStride);
        }
      } else {
        for (TreeReaderFactory.TreeReader eachReader : reader) {
          eachReader.skipRows(nextRowInStripe - rowInStripe);
        }
      }
      rowInStripe = nextRowInStripe;
    }
    return true;
  }

  public boolean hasNext() throws IOException {
    return rowInStripe < rowCountInStripe;
  }

  public Tuple next() throws IOException {
    if (hasNext()) {
      try {
        for (int i = 0; i < reader.length; i++) {
          result.put(i, reader[i].next());
        }
        // find the next row
        rowInStripe += 1;
        advanceToNextRow(reader, rowInStripe + rowBaseInStripe, true);
        return result;
      } catch (IOException e) {
        // Rethrow exception with file name in log message
        throw new IOException("Error reading file: " + path, e);
      }
    } else {
      return null;
    }
  }

  /**
   * Read the next stripe until we find a row that we don't skip.
   *
   * @throws IOException
   */
  private void advanceStripe() throws IOException {
    rowInStripe = rowCountInStripe;
    while (rowInStripe >= rowCountInStripe &&
        currentStripe < stripes.size() - 1) {
      currentStripe += 1;
      readStripe();
    }
  }

  /**
   * Read the current stripe into memory.
   *
   * @throws IOException
   */
  private void readStripe() throws IOException {
    StripeInformation stripe = beginReadStripe();

    // if we haven't skipped the whole stripe, read the data
    if (rowInStripe < rowCountInStripe) {
      // if we aren't projecting columns or filtering rows, just read it all
      if (included == null) {
        readAllDataStreams(stripe);
      } else {
        readPartialDataStreams(stripe);
      }

      for (TreeReaderFactory.TreeReader eachReader : reader) {
        eachReader.startStripe(streams, stripeFooter);
      }
      // if we skipped the first row group, move the pointers forward
      if (rowInStripe != 0) {
        seekToRowEntry(reader, (int) (rowInStripe / rowIndexStride));
      }
    }
  }

  private void clearStreams() throws IOException {
    // explicit close of all streams to de-ref ByteBuffers
    for (InStream is : streams.values()) {
      is.close();
    }
    if (bufferChunks != null) {
      if (dataReader.isTrackingDiskRanges()) {
        for (DiskRangeList range = bufferChunks; range != null; range = range.next) {
          if (!(range instanceof BufferChunk)) {
            continue;
          }
          dataReader.releaseBuffer(((BufferChunk) range).getChunk());
        }
      }
    }
    bufferChunks = null;
    streams.clear();
  }

  OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException {
    return metadata.readStripeFooter(stripe);
  }

  private StripeInformation beginReadStripe() throws IOException {
    StripeInformation stripe = stripes.get(currentStripe);
    stripeFooter = readStripeFooter(stripe);
    clearStreams();
    // setup the position in the stripe
    rowCountInStripe = stripe.getNumberOfRows();
    rowInStripe = 0;
    rowBaseInStripe = 0;
    for (int i = 0; i < currentStripe; ++i) {
      rowBaseInStripe += stripes.get(i).getNumberOfRows();
    }
    // reset all of the indexes
    for (int i = 0; i < indexes.length; ++i) {
      indexes[i] = null;
    }
    return stripe;
  }

  private void readAllDataStreams(StripeInformation stripe) throws IOException {
    long start = stripe.getIndexLength();
    long end = start + stripe.getDataLength();
    // explicitly trigger 1 big read
    DiskRangeList toRead = new DiskRangeList(start, end);
    bufferChunks = dataReader.readFileData(toRead, stripe.getOffset(), false);
    List<OrcProto.Stream> streamDescriptions = stripeFooter.getStreamsList();
    createStreams(streamDescriptions, bufferChunks, included, codec, bufferSize, streams);
  }

  public long getRowNumber() {
    return rowInStripe + rowBaseInStripe + firstRow;
  }

  public float getProgress() {
    return ((float) rowBaseInStripe + rowInStripe) / totalRowCount;
  }

  private int findStripe(long rowNumber) {
    for (int i = 0; i < stripes.size(); i++) {
      StripeInformation stripe = stripes.get(i);
      if (stripe.getNumberOfRows() > rowNumber) {
        return i;
      }
      rowNumber -= stripe.getNumberOfRows();
    }
    throw new IllegalArgumentException("Seek after the end of reader range");
  }

  OrcIndex readRowIndex(
      int stripeIndex, boolean[] included) throws IOException {
    return readRowIndex(stripeIndex, included, null, null);
  }

  OrcIndex readRowIndex(int stripeIndex, boolean[] included, OrcProto.RowIndex[] indexes,
                        OrcProto.BloomFilterIndex[] bloomFilterIndex) throws IOException {
    StripeInformation stripe = stripes.get(stripeIndex);
    OrcProto.StripeFooter stripeFooter = null;
    // if this is the current stripe, use the cached objects.
    if (stripeIndex == currentStripe) {
      stripeFooter = this.stripeFooter;
      indexes = indexes == null ? this.indexes : indexes;
      bloomFilterIndex = bloomFilterIndex == null ? this.bloomFilterIndices : bloomFilterIndex;
    }
    return metadata.readRowIndex(stripe, stripeFooter, included, indexes, null,
        bloomFilterIndex);
  }

  private void seekToRowEntry(TreeReaderFactory.TreeReader []reader, int rowEntry)
      throws IOException {
    PositionProvider[] index = new PositionProvider[indexes.length];
    for (int i = 0; i < indexes.length; ++i) {
      if (indexes[i] != null) {
        index[i] = new PositionProviderImpl(indexes[i].getEntry(rowEntry));
      }
    }
    for (TreeReaderFactory.TreeReader eachReader : reader) {
      eachReader.seek(index);
    }
  }

  public void seekToRow(long rowNumber) throws IOException {
    if (rowNumber < 0) {
      throw new IllegalArgumentException("Seek to a negative row number " +
          rowNumber);
    } else if (rowNumber < firstRow) {
      throw new IllegalArgumentException("Seek before reader range " +
          rowNumber);
    }
    // convert to our internal form (rows from the beginning of slice)
    rowNumber -= firstRow;

    // move to the right stripe
    int rightStripe = findStripe(rowNumber);
    if (rightStripe != currentStripe) {
      currentStripe = rightStripe;
      readStripe();
    }
    readRowIndex(currentStripe, included);

    // if we aren't to the right row yet, advance in the stripe.
    advanceToNextRow(reader, rowNumber, true);
  }

  public long getNumBytes() {
    return ((RecordReaderUtils.DefaultDataReader)dataReader).getReadBytes();
  }

  @Override
  public void close() throws IOException {
    clearStreams();
    dataReader.close();
  }

  public static final class PositionProviderImpl implements PositionProvider {
    private final OrcProto.RowIndexEntry entry;
    private int index;

    public PositionProviderImpl(OrcProto.RowIndexEntry entry) {
      this(entry, 0);
    }

    public PositionProviderImpl(OrcProto.RowIndexEntry entry, int startPos) {
      this.entry = entry;
      this.index = startPos;
    }

    @Override
    public long getNext() {
      return entry.getPositions(index++);
    }
  }
}
