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

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.TableMeta;
import tajo.catalog.TableMetaImpl;
import tajo.catalog.proto.CatalogProtos.TableProto;
import tajo.datum.Datum;
import tajo.storage.exception.UnknownCodecException;
import tajo.storage.exception.UnknownDataTypeException;
import tajo.storage.hcfile.HCFile.Scanner;
import tajo.util.FileUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HColumnReader implements ColumnReader {
  private final Log LOG = LogFactory.getLog(HColumnReader.class);
  private final Configuration conf;
  private final FileSystem fs;
  private Scanner scanner;
  private Path dataDir;
  private List<Path> dataPaths = Lists.newArrayList();
  private int next;
  private Index<Integer> index;
  private Column target;

  public HColumnReader(Configuration conf, Path tablePath, int targetColumnIdx)
      throws IOException {
    this.conf = conf;
    this.fs = tablePath.getFileSystem(this.conf);
    Schema schema = getSchema(tablePath);
    init(conf, tablePath, schema.getColumn(targetColumnIdx));
  }

  public HColumnReader(Configuration conf, Path tablePath, String columnName)
      throws IOException {
    this.conf = conf;
    this.fs = tablePath.getFileSystem(this.conf);
    Schema schema = getSchema(tablePath);
    init(conf, tablePath, schema.getColumnByName(columnName));
  }

  public HColumnReader(Configuration conf, Path tablePath, Column target)
      throws IOException {
    this.conf = conf;
    this.fs = tablePath.getFileSystem(this.conf);
    init(conf, tablePath, target);
  }

  public List<Path> getDataPaths() {
    return this.dataPaths;
  }

  private void init(Configuration conf, Path tablePath, Column target) throws IOException {
    this.dataDir = new Path(tablePath, "data");
    FileStatus[] files = fs.listStatus(dataDir);
    Path[] shardPaths = new Path[files.length];
    int i, j = 0;
    for (i = 0; i < files.length; i++) {
      shardPaths[j++] = files[i].getPath();
    }
    Arrays.sort(shardPaths);
//    next = 0;
    index = new Index<Integer>();
    this.target = target;
    initDataPaths(shardPaths);
  }

  private void setTarget(Path tablePath, int targetColumnIdx) throws IOException {
    Schema schema = getSchema(tablePath);
    this.target = schema.getColumn(targetColumnIdx);
  }

  private void setTarget(Path tablePath, String columnName) throws IOException {
    Schema schema = getSchema(tablePath);
    this.target = schema.getColumn(columnName);
  }

  private Schema getSchema(Path tablePath) throws IOException {
    Path metaPath = new Path(tablePath, ".meta");
    TableProto proto = (TableProto) FileUtil.loadProto(fs, metaPath,
        TableProto.getDefaultInstance());
    TableMeta meta = new TableMetaImpl(proto);
    return meta.getSchema();
  }

  private void initDataPaths(Path[] shardPaths) throws IOException {
    Path indexPath;
    FSDataInputStream in;
    long rid;
    String targetName = target.getColumnName();

    for (int i = 0; i < shardPaths.length; i++) {
      indexPath = new Path(shardPaths[i], ".smeta");
      in = fs.open(indexPath);

      rid = in.readLong();
      index.add(new IndexItem(rid, i));

      in.close();

      FileStatus[] columnFiles = fs.listStatus(shardPaths[i]);
      for (FileStatus file : columnFiles) {
        String colName = file.getPath().getName();
        if (colName.length() < targetName.length()) {
          continue;
        }
        if (colName.substring(0, targetName.length()).equals(targetName)) {
          LOG.info("column file: " + file.getPath().toString());
          dataPaths.add(file.getPath());
        }
      }
    }
    next = 0;
  }

  @Override
  public void first() throws IOException {
    pos(0);
  }

  @Override
  public void last() throws IOException {
    // TODO
  }

  @Override
  public Datum get() throws IOException {
    Datum datum;

    if (scanner == null) {
      if (next < dataPaths.size()) {
        scanner = getScanner(conf, dataPaths.get(next++));
        return scanner.get();
      } else {
        return null;
      }
    } else {
      datum = scanner.get();
      if (datum == null) {
        scanner.close();
        scanner = null;
        if (next < dataPaths.size()) {
          scanner = getScanner(conf, dataPaths.get(next++));
          return scanner.get();
        } else {
          return null;
        }
      }
      return datum;
    }
  }

  @Override
  public void pos(long rid) throws IOException {
    if (scanner != null) {
      HColumnMetaWritable meta = (HColumnMetaWritable) scanner.getMeta();
      if (meta.getStartRid() <= rid
          && meta.getStartRid() + meta.getRecordNum() > rid) {
        scanner.pos(rid);
        return;
      } else {
        scanner.close();
        scanner = null;
      }
    }

    // scanner must be null
    IndexItem<Integer> item = index.searchLargestSmallerThan(rid);
    if (item == null) {
      throw new IOException("Cannot find the column file containing " + rid);
    }
    Path shardPath = new Path(dataDir, item.getValue().toString());

    int i;
    FileStatus[] files = fs.listStatus(shardPath);
    for (FileStatus file : files) {
      if (dataPaths.contains(file.getPath())) {
        LOG.info("matched path: " + file.getPath());
        scanner = getScanner(conf, file.getPath());
        HColumnMetaWritable meta = (HColumnMetaWritable) scanner.getMeta();
        LOG.info("start: " + meta.getStartRid() + " len: " + meta.getRecordNum());
        if (meta.getStartRid() <= rid
            && meta.getStartRid() + meta.getRecordNum() > rid) {
          scanner.pos(rid);

          for (i = 0; i < dataPaths.size(); i++) {
            if (file.getPath().equals(dataPaths.get(i))) {
              next = i+1;
              break;
            }
          }
          if (i == dataPaths.size()) {
            throw new IOException("Invalid path: " + file.getPath().toString());
          }
          return;
        }
        scanner.close();
        scanner = null;
      }
    }

    if (scanner == null) {
      throw new IOException("Null scanner");
    }
  }

  private Scanner getScanner(Configuration conf, Path columnPath) throws IOException {
    try {
      scanner = new Scanner(conf, columnPath);
      return scanner;
    } catch (UnknownDataTypeException e) {
      throw new IOException(e);
    } catch (UnknownCodecException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getPos() throws IOException {
    // TODO
    return scanner.getPos();
  }

  @Override
  public void close() throws  IOException {
    if (scanner != null) {
      scanner.close();
    }
  }
}
