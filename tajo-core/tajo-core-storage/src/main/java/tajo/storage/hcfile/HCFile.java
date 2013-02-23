/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.storage.hcfile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import tajo.catalog.proto.CatalogProtos.CompressType;
import tajo.datum.Datum;
import tajo.storage.exception.UnknownCodecException;
import tajo.storage.exception.UnknownDataTypeException;
import tajo.storage.hcfile.reader.Reader;
import tajo.storage.hcfile.writer.Writer;

import java.io.IOException;
import java.nio.ByteBuffer;

public class HCFile {

  public static class Scanner extends ColumnFileScanner {
    private final Log LOG = LogFactory.getLog(Scanner.class);
    public static final int HDFS_CHUNK_SIZE = 65536;
    private HColumnMetaWritable meta;
    private FileSystem fs;
    private FSDataInputStream in;
    private ByteBuffer buffer;
    private UpdatableSeekableBlock block;
    private Reader reader;
    private Index<Long> index;
    private int nextBlock;

    public Scanner(Configuration conf, Path path)
        throws IOException, UnknownDataTypeException, UnknownCodecException {
      super(conf, path);
      fs = path.getFileSystem(conf);
      in = fs.open(path);
      buffer = ByteBuffer.allocateDirect(HDFS_CHUNK_SIZE);
      buffer.flip();
      index = new Index<Long>();
      FileStatus file = fs.getFileStatus(path);

      readColumnHeader(file.getLen());
      if (this.meta.isCompressed()) {
        reader = new CompressedBlockReader(this.meta.getDataType(), this.meta.getCompressType());
      } else {
        reader = new BlockReader(this.meta.getDataType());
      }
    }

    public Path getPath() {
      return this.path;
    }

    protected boolean fillBuffer(int desired) throws IOException {
      buffer.compact();
      int read = in.read(buffer);
      if (read == -1) {
        return false;
      } else {
        if (read < desired) {
          read += in.read(buffer);
          if (read < desired) {
            return false;
          }
        }
        buffer.flip();
        return true;
      }
    }

    private void readColumnHeader(long fileLen) throws IOException {
      in.seek(fileLen - HColumnMetaWritable.size());
      long headerPos = in.getPos();
      meta = new HColumnMetaWritable();
      meta.readFields(in);

      // read index
      in.seek(meta.getOffsetToIndex());
      while (in.getPos() < headerPos) {
        index.add(new IndexItem<Long>(in.readLong(), in.readLong()));
      }

      in.seek(0);
      nextBlock = 0;
      block = null;
    }

//    private void readColumnHeader(long fileLen) throws IOException {
//      in.seek(fileLen - Long.SIZE / 8);
//      long headerPos = in.readLong();
//      in.seek(headerPos);
//
//      // read header
//      long headerLen = fileLen - Long.SIZE/8 - headerPos;
//      byte[] buf = new byte[(int)headerLen];
//      in.read(buf);
//      ColumnMetaProto proto = ColumnMetaProto.parseFrom(buf);
//      meta = new HCFileMetaImpl(proto);
//
//      // read index
//      in.seek(((HCFileMetaImpl)meta).getOffsetToIndex());
//      while (in.getPos() < headerPos) {
//        index.add(new IndexItem(in.readLong(), in.readLong()));
//      }
//
//      in.seek(0);
//      currentBlock = 0;
//      block = null;
//    }

    @Override
    public ColumnMeta getMeta() throws IOException {
      return this.meta;
    }

    @Override
    public void first() throws IOException {
      if (nextBlock != 1) {
        pos(0);
      }
    }

    @Override
    public void last() throws IOException {
      if (nextBlock != index.size()) {
        IndexItem<Long> item = index.get(index.size()-1);
        long lastOffset = item.getValue();
        in.seek(lastOffset);
        nextBlock = index.size()-1;
        block = null;
        buffer.clear();
        buffer.flip();
        getBlock();
        block.last();
      }
    }

    @Override
    public void pos(long rid) throws IOException {
      // Consider the case which the given rid is already involved in the block
      if (block == null
          || block.getMeta().getStartRid() > rid
          || block.getMeta().getStartRid()+block.getMeta().getRecordNum() <= rid) {
        IndexItem<Long> item = index.searchLargestSmallerThan(rid);
        long offset = item.getValue();
        in.seek(offset);
        nextBlock = index.findPos(item);
        block = null;
        // block을 읽을 경우 buffer를 완전히 비워야 함
        buffer.clear();
        buffer.flip();
        getBlock();
      }

      block.pos(rid-block.getMeta().getStartRid());
    }

    @Override
    public Datum get() throws IOException {
      Datum ret;
      if (block == null || (ret=block.next()) == null) {
        getBlock();
        if (block == null) {
          return null;
        }
        ret = block.next();
      }
      return ret;
    }

    @Override
    public Block getBlock() throws IOException {
      if (nextBlock < index.size()) {
        if (buffer.remaining() < Integer.SIZE/8) {
          if (!fillBuffer(Integer.SIZE/8-buffer.remaining())) {
            return null;
          }
        }
        int blockSize = buffer.getInt();
        if (buffer.remaining() < blockSize) {
          if (!fillBuffer(blockSize-buffer.remaining())) {
            return null;
          }
        }

        int originLimit = buffer.limit();
        buffer.limit(buffer.position() + blockSize);
        BlockMeta blockMeta = newBlockMeta();

        block = (UpdatableSeekableBlock) reader.read(blockMeta, buffer);
        buffer.limit(originLimit);
        nextBlock++;
        return block;
      } else {
        return null;
      }
    }

    private BlockMeta newBlockMeta() throws IOException {
      return new HBlockMetaImpl(
          meta.getDataType(),
          0, // not set
          index.get(nextBlock).getRid(),
          meta.isSorted(),
          meta.isContiguous(),
          meta.isCompressed());
    }

    @Override
    public Datum[] getBlockAsArray() throws IOException {
      return this.getBlock().asArray();
    }

    @Override
    public long getPos() throws IOException {
      // TODO
      return block.getMeta().getStartRid() + block.getPos();
    }

    @Override
    public void close() throws IOException {
      reader.close();
      in.close();
    }
  }

  public static class Appender extends ColumnFileAppender {
    private Log LOG = LogFactory.getLog(Appender.class);
    private final int HDFS_BLOCK_SIZE;
    private FSDataOutputStream out;
    private FileSystem fs;
    private UpdatableBlock block;
    private BlockMeta blockMeta;
    private long blockStartId;
    private long startId;
    private int blockRecordNum;
    private int totalRecordNum;
    private Writer writer;
    private Index<Long> index;

    public Appender(Configuration conf, ColumnMeta meta, Path path)
        throws IOException, UnknownDataTypeException, UnknownCodecException {
      super(conf, meta, path);
      HDFS_BLOCK_SIZE = conf.getInt("dfs.blocksize", -1);
      if (HDFS_BLOCK_SIZE == -1) {
        throw new IOException("HDFS block size can not be initialized!");
      } else {
        LOG.info("HDFS block size: " + HDFS_BLOCK_SIZE);
      }
      fs = path.getFileSystem(conf);
      out = fs.create(path);

      if (this.meta.getCompressType() != CompressType.COMP_NONE) {
        writer = new CompressedBlockWriter(out,
            this.meta.getDataType(), this.meta.getCompressType());
      } else {
        writer = new BlockWriter(out, this.meta.getDataType());
        blockMeta = new HBlockMetaImpl(this.meta.getDataType(), 0, 0,
            this.meta.isSorted(), this.meta.isContiguous(), this.meta.isCompressed());
        block = new BasicBlock();
      }
      index = new Index<Long>();
      blockStartId = ((HColumnMetaWritable)this.meta).getStartRid();
      startId = blockStartId;
      blockRecordNum = 0;
      totalRecordNum = 0;
    }

    public Path getPath() {
      return this.path;
    }

    public boolean isAppendable(Datum datum) throws IOException {
      // TODO: validation of (index.size()+1)
      int indexSize = (index.size()+1) * 2 * Long.SIZE/8;
      if (!block.isAppendable(datum)) {
        indexSize += 2 * Long.SIZE/8;
      }

      return out.getPos() /* written size */
          + block.getSize()
          + Integer.SIZE/8 /* block size */
          + ColumnStoreUtil.getWrittenSize(datum)
          + HColumnMetaWritable.size()
          + Long.SIZE/8 /* column header pos */
          + indexSize
          < HDFS_BLOCK_SIZE;
    }

    public long getStartId() {
      return this.startId;
    }

    public int getRecordNum() {
      return this.totalRecordNum;
    }

    @Override
    public void append(Datum datum) throws IOException {
      if (!block.isAppendable(datum)) {
        flush();
        blockRecordNum = 0;
      }
      block.appendValue(datum);
      blockRecordNum++;
      totalRecordNum++;
    }

    @Override
    public void flush() throws IOException {
      index.add(new IndexItem(blockStartId, writer.getPos()));
      blockMeta.setStartRid(blockStartId)
          .setRecordNum(blockRecordNum);
      block.setMeta(blockMeta);
      writer.write(block);
      block.clear();
      blockStartId += blockRecordNum;
    }

    private void writeColumnHeader() throws IOException {
      long offsetToIndex = out.getPos();
      // write index
      for (IndexItem<Long> e : index.get()) {
        out.writeLong(e.getRid());
        out.writeLong(e.getValue());
      }

      // write header
      HColumnMetaWritable columnMeta = (HColumnMetaWritable) meta;
      columnMeta.setRecordNum(totalRecordNum);
      columnMeta.setOffsetToIndex((int) offsetToIndex);
      columnMeta.write(out);

//      long headerPos = out.getPos();
//
//      HCFileMetaImpl hmeta = (HCFileMetaImpl) meta;
//      hmeta.setRecordNum(totalRecordNum);
//      hmeta.setOffsetToIndex((int)offsetToIndex);
//      ColumnMetaProto proto = meta.getProto();
//      FileUtil.writeProto(out, proto);
//
//      out.writeLong(headerPos);
    }

    @Override
    public void close() throws IOException {
      if (blockRecordNum > 0) {
        flush();
      }
      writeColumnHeader();

      writer.close();
      out.close();
    }
  }
}
