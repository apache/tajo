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

package org.apache.tajo.storage.orc;

import com.google.common.collect.Lists;
import com.google.protobuf.CodedInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.orc.*;
import org.apache.orc.Reader.Options;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.InStream;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.storage.FileScanner;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.thirdparty.orc.OrcRecordReader;
import org.apache.tajo.storage.thirdparty.orc.OrcUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class OrcScanner extends FileScanner {
  private static final Log LOG = LogFactory.getLog(OrcScanner.class);

  private static final int DIRECTORY_SIZE_GUESS = 16 * 1024;

  protected final FileSystem fileSystem;
  private final long maxLength = Long.MAX_VALUE;
  protected final Path path;
  protected org.apache.orc.CompressionKind compressionKind;
  protected CompressionCodec codec;
  protected int bufferSize;
  private List<OrcProto.StripeStatistics> stripeStats;
  private int metadataSize;
  protected List<OrcProto.Type> types;
  private List<OrcProto.UserMetadataItem> userMetadata;
  private List<OrcProto.ColumnStatistics> fileStats;
  private List<StripeInformation> stripes;
  protected int rowIndexStride;
  private long contentLength, numberOfRows;
  private TimeZone timeZone;

  private List<Integer> versionList;

  //serialized footer - Keeping this around for use by getFileMetaInfo()
  // will help avoid cpu cycles spend in deserializing at cost of increased
  // memory footprint.
  private ByteBuffer footerByteBuffer;
  // Same for metastore cache - maintains the same background buffer, but includes postscript.
  // This will only be set if the file footer/metadata was read from disk.
  private ByteBuffer footerMetaAndPsBuffer;

  private OrcRecordReader recordReader;

  private long recordCount = 0;

  /**
   * Ensure this is an ORC file to prevent users from trying to read text
   * files or RC files as ORC files.
   * @param in the file being read
   * @param path the filename for error messages
   * @param psLen the postscript length
   * @param buffer the tail of the file
   * @throws IOException
   */
  static void ensureOrcFooter(FSDataInputStream in,
                              Path path,
                              int psLen,
                              ByteBuffer buffer) throws IOException {
    int len = OrcFile.MAGIC.length();
    if (psLen < len + 1) {
      throw new IOException("Malformed ORC file " + path +
          ". Invalid postscript length " + psLen);
    }
    int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - 1 - len;
    byte[] array = buffer.array();
    // now look for the magic string at the end of the postscript.
    if (!Text.decode(array, offset, len).equals(OrcFile.MAGIC)) {
      // If it isn't there, this may be the 0.11.0 version of ORC.
      // Read the first 3 bytes of the file to check for the header
      byte[] header = new byte[len];
      in.readFully(0, header, 0, len);
      // if it isn't there, this isn't an ORC file
      if (!Text.decode(header, 0 , len).equals(OrcFile.MAGIC)) {
        throw new IOException("Malformed ORC file " + path +
            ". Invalid postscript.");
      }
    }
  }

  /**
   * Build a version string out of an array.
   * @param version the version number as a list
   * @return the human readable form of the version string
   */
  private static String versionString(List<Integer> version) {
    StringBuilder buffer = new StringBuilder();
    for(int i=0; i < version.size(); ++i) {
      if (i != 0) {
        buffer.append('.');
      }
      buffer.append(version.get(i));
    }
    return buffer.toString();
  }

  /**
   * Check to see if this ORC file is from a future version and if so,
   * warn the user that we may not be able to read all of the column encodings.
   * @param log the logger to write any error message to
   * @param path the data source path for error messages
   * @param version the version of hive that wrote the file.
   */
  static void checkOrcVersion(Log log, Path path, List<Integer> version) {
    if (version.size() >= 1) {
      int major = version.get(0);
      int minor = 0;
      if (version.size() >= 2) {
        minor = version.get(1);
      }
      if (major > OrcFile.Version.CURRENT.getMajor() ||
          (major == OrcFile.Version.CURRENT.getMajor() &&
              minor > OrcFile.Version.CURRENT.getMinor())) {
        log.warn(path + " was written by a future Hive version " +
            versionString(version) +
            ". This file may not be readable by this version of Hive.");
      }
    }
  }

  public OrcScanner(Configuration conf, Schema schema, TableMeta meta, Fragment fragment) throws IOException {
    super(conf, schema, meta, fragment);

    this.path = this.fragment.getPath();
    this.fileSystem = this.path.getFileSystem(conf);
  }

  private static FileMetaInfo extractMetaInfoFromFooter(FileSystem fs,
                                                        Path path,
                                                        long maxFileLength
  ) throws IOException {
    FSDataInputStream file = fs.open(path);

    // figure out the size of the file using the option or filesystem
    long size;
    if (maxFileLength == Long.MAX_VALUE) {
      size = fs.getFileStatus(path).getLen();
    } else {
      size = maxFileLength;
    }

    //read last bytes into buffer to get PostScript
    int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
    ByteBuffer buffer = ByteBuffer.allocate(readSize);
    assert buffer.position() == 0;
    file.readFully((size - readSize),
        buffer.array(), buffer.arrayOffset(), readSize);
    buffer.position(0);

    //read the PostScript
    //get length of PostScript
    int psLen = buffer.get(readSize - 1) & 0xff;
    ensureOrcFooter(file, path, psLen, buffer);
    int psOffset = readSize - 1 - psLen;
    OrcProto.PostScript ps = extractPostScript(buffer, path, psLen, psOffset);

    int footerSize = (int) ps.getFooterLength();
    int metadataSize = (int) ps.getMetadataLength();

    //check if extra bytes need to be read
    ByteBuffer fullFooterBuffer = null;
    int extra = Math.max(0, psLen + 1 + footerSize + metadataSize - readSize);
    if (extra > 0) {
      //more bytes need to be read, seek back to the right place and read extra bytes
      ByteBuffer extraBuf = ByteBuffer.allocate(extra + readSize);
      file.readFully((size - readSize - extra), extraBuf.array(),
          extraBuf.arrayOffset() + extraBuf.position(), extra);
      extraBuf.position(extra);
      //append with already read bytes
      extraBuf.put(buffer);
      buffer = extraBuf;
      buffer.position(0);
      fullFooterBuffer = buffer.slice();
      buffer.limit(footerSize + metadataSize);
    } else {
      //footer is already in the bytes in buffer, just adjust position, length
      buffer.position(psOffset - footerSize - metadataSize);
      fullFooterBuffer = buffer.slice();
      buffer.limit(psOffset);
    }

    // remember position for later
    buffer.mark();

    file.close();

    return new FileMetaInfo(
        ps.getCompression().toString(),
        (int) ps.getCompressionBlockSize(),
        (int) ps.getMetadataLength(),
        buffer,
        ps.getVersionList(),
        org.apache.orc.OrcFile.WriterVersion.FUTURE,
        fullFooterBuffer
    );
  }

  public OrcRecordReader createRecordReader() throws IOException {
    return new OrcRecordReader(this.stripes, fileSystem, schema, targets, fragment, types, codec, bufferSize,
        rowIndexStride, buildReaderOptions(meta), conf,
        timeZone);
  }

  private static Options buildReaderOptions(TableMeta meta) {
    return new Options()
        .useZeroCopy(Boolean.parseBoolean(meta.getProperty(OrcConf.USE_ZEROCOPY.getAttribute(),
            String.valueOf(OrcConf.USE_ZEROCOPY.getDefaultValue()))))
        .skipCorruptRecords(Boolean.parseBoolean(meta.getProperty(OrcConf.SKIP_CORRUPT_DATA.getAttribute(),
            String.valueOf(OrcConf.SKIP_CORRUPT_DATA.getDefaultValue()))));
  }

  @Override
  public void init() throws IOException {
    FileMetaInfo footerMetaData = extractMetaInfoFromFooter(fileSystem, path, maxLength);
    this.footerMetaAndPsBuffer = footerMetaData.footerMetaAndPsBuffer;
    MetaInfoObjExtractor rInfo =
        new MetaInfoObjExtractor(footerMetaData.compressionType,
            footerMetaData.bufferSize,
            footerMetaData.metadataSize,
            footerMetaData.footerBuffer
        );
    this.footerByteBuffer = footerMetaData.footerBuffer;
    this.compressionKind = rInfo.compressionKind;
    this.codec = rInfo.codec;
    this.bufferSize = rInfo.bufferSize;
    this.metadataSize = rInfo.metadataSize;
    this.stripeStats = rInfo.metadata.getStripeStatsList();
    this.types = rInfo.footer.getTypesList();
    this.rowIndexStride = rInfo.footer.getRowIndexStride();
    this.contentLength = rInfo.footer.getContentLength();
    this.numberOfRows = rInfo.footer.getNumberOfRows();
    this.userMetadata = rInfo.footer.getMetadataList();
    this.fileStats = rInfo.footer.getStatisticsList();
    this.versionList = footerMetaData.versionList;
    this.stripes = convertProtoStripesToStripes(rInfo.footer.getStripesList());

    this.timeZone = TimeZone.getTimeZone(meta.getProperty(StorageConstants.TIMEZONE,
        StorageUtil.TAJO_CONF.getSystemTimezone().getID()));
    this.recordReader = createRecordReader();

    super.init();
  }

  @Override
  public Tuple next() throws IOException {
    Tuple next = recordReader.next();
    if (next != null) {
      recordCount++;
    }
    return next;
  }

  @Override
  public void reset() throws IOException {
    // TODO: improve this
    this.close();
    recordReader = createRecordReader();
  }

  @Override
  public void close() throws IOException {
    if (recordReader != null) {
      recordReader.close();
      inputStats.setNumBytes(recordReader.getNumBytes());
      inputStats.setNumRows(recordCount);
    }
  }

  @Override
  public boolean isProjectable() {
    return true;
  }

  @Override
  public boolean isSelectable() {
    return false;
  }

  @Override
  public void setFilter(EvalNode filter) {
    // TODO: implement this
  }

  @Override
  public float getProgress() {
    return inited ? recordReader.getProgress() : super.getProgress();
  }

  @Override
  public boolean isSplittable() {
    return true;
  }

  private static OrcProto.PostScript extractPostScript(ByteBuffer bb, Path path,
                                                       int psLen, int psAbsOffset) throws IOException {
    // TODO: when PB is upgraded to 2.6, newInstance(ByteBuffer) method should be used here.
    assert bb.hasArray();
    CodedInputStream in = CodedInputStream.newInstance(
        bb.array(), bb.arrayOffset() + psAbsOffset, psLen);
    OrcProto.PostScript ps = OrcProto.PostScript.parseFrom(in);
    checkOrcVersion(LOG, path, ps.getVersionList());

    // Check compression codec.
    switch (ps.getCompression()) {
      case NONE:
        break;
      case ZLIB:
        break;
      case SNAPPY:
        break;
      case LZO:
        break;
      default:
        throw new IllegalArgumentException("Unknown compression");
    }
    return ps;
  }

  private static OrcProto.Footer extractFooter(ByteBuffer bb, int footerAbsPos,
                                               int footerSize, CompressionCodec codec, int bufferSize) throws IOException {
    bb.position(footerAbsPos);
    bb.limit(footerAbsPos + footerSize);
    return OrcProto.Footer.parseFrom(InStream.createCodedInputStream("footer",
        Lists.newArrayList(new BufferChunk(bb, 0)), footerSize, codec, bufferSize));
  }

  private static OrcProto.Metadata extractMetadata(ByteBuffer bb, int metadataAbsPos,
                                                   int metadataSize, CompressionCodec codec, int bufferSize) throws IOException {
    bb.position(metadataAbsPos);
    bb.limit(metadataAbsPos + metadataSize);
    return OrcProto.Metadata.parseFrom(InStream.createCodedInputStream("metadata",
        Lists.newArrayList(new BufferChunk(bb, 0)), metadataSize, codec, bufferSize));
  }

  /**
   * MetaInfoObjExtractor - has logic to create the values for the fields in ReaderImpl
   *  from serialized fields.
   * As the fields are final, the fields need to be initialized in the constructor and
   *  can't be done in some helper function. So this helper class is used instead.
   *
   */
  private static class MetaInfoObjExtractor{
    final org.apache.orc.CompressionKind compressionKind;
    final CompressionCodec codec;
    final int bufferSize;
    final int metadataSize;
    final OrcProto.Metadata metadata;
    final OrcProto.Footer footer;

    MetaInfoObjExtractor(String codecStr, int bufferSize, int metadataSize,
                         ByteBuffer footerBuffer) throws IOException {

      this.compressionKind = org.apache.orc.CompressionKind.valueOf(codecStr);
      this.bufferSize = bufferSize;
      this.codec = OrcUtils.createCodec(compressionKind);
      this.metadataSize = metadataSize;

      int position = footerBuffer.position();
      int footerBufferSize = footerBuffer.limit() - footerBuffer.position() - metadataSize;

      this.metadata = extractMetadata(footerBuffer, position, metadataSize, codec, bufferSize);
      this.footer = extractFooter(
          footerBuffer, position + metadataSize, footerBufferSize, codec, bufferSize);

      footerBuffer.position(position);
    }
  }

  public static class StripeInformationImpl
      implements org.apache.orc.StripeInformation {
    private final OrcProto.StripeInformation stripe;

    public StripeInformationImpl(OrcProto.StripeInformation stripe) {
      this.stripe = stripe;
    }

    @Override
    public long getOffset() {
      return stripe.getOffset();
    }

    @Override
    public long getLength() {
      return stripe.getDataLength() + getIndexLength() + getFooterLength();
    }

    @Override
    public long getDataLength() {
      return stripe.getDataLength();
    }

    @Override
    public long getFooterLength() {
      return stripe.getFooterLength();
    }

    @Override
    public long getIndexLength() {
      return stripe.getIndexLength();
    }

    @Override
    public long getNumberOfRows() {
      return stripe.getNumberOfRows();
    }

    @Override
    public String toString() {
      return "offset: " + getOffset() + " data: " + getDataLength() +
          " rows: " + getNumberOfRows() + " tail: " + getFooterLength() +
          " index: " + getIndexLength();
    }
  }

  private static List<StripeInformation> convertProtoStripesToStripes(
      List<OrcProto.StripeInformation> stripes) {
    List<StripeInformation> result = new ArrayList<>(stripes.size());
    for (OrcProto.StripeInformation info : stripes) {
      result.add(new StripeInformationImpl(info));
    }
    return result;
  }

}
