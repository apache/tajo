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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.datum.Datum;
import tajo.storage.hcfile.HCFile.Scanner;
import tajo.storage.exception.UnknownCodecException;
import tajo.storage.exception.UnknownDataTypeException;

import java.io.IOException;

public class HColumnReader implements ColumnReader {
  private Log LOG = LogFactory.getLog(HColumnReader.class);
  private Scanner scanner;
  private final Configuration conf;
  private final FileSystem fs;
  private final Path columnPath;
  private Path[] dataPaths;
  private short next;
  private Index<String> index;

  public HColumnReader(Configuration conf, Path columnPath)
      throws IOException {
    this.conf = conf;
    fs = columnPath.getFileSystem(this.conf);
    this.columnPath = columnPath;
    FileStatus[] files = fs.listStatus(columnPath);
    dataPaths = new Path[files.length-1];
    int i, j = 0;
    for (i = 0; i < files.length; i++) {
      if (files[i].getPath().getName().equals(".index")) {
        continue;
      }
      dataPaths[j++] = files[i].getPath();
    }
    next = 0;
    index = null;
  }

  private void init(Path columnPath) throws IOException {
    Path indexPath = new Path(columnPath, ".index");
    FSDataInputStream in = fs.open(indexPath);

    long rid;
    short len;
    byte[] bytes;
    while (in.available() > 0) {
      rid = in.readLong();
      len = in.readShort();
      bytes = new byte[len];
      in.read(bytes);
      index.add(new IndexItem(rid, new String(bytes)));
    }

    in.close();
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
      try {
        scanner = new Scanner(conf, dataPaths[next++]);
        return scanner.get();
      } catch (UnknownDataTypeException e) {
        throw new IOException(e);
      } catch (UnknownCodecException e) {
        throw new IOException(e);
      }
    } else {
      datum = scanner.get();
      if (datum == null) {
        scanner.close();
        scanner = null;
        if (next < dataPaths.length) {
          try {
            scanner = new Scanner(conf, dataPaths[next++]);
          } catch (UnknownDataTypeException e) {
            throw new IOException(e);
          } catch (UnknownCodecException e) {
            throw new IOException(e);
          }
          datum = scanner.get();
        } else {
          datum = null;
        }
      }
      return datum;
    }
  }

  @Override
  public void pos(long rid) throws IOException {
    if (index == null) {
      index = new Index<>();
      init(columnPath);
    }
    IndexItem<String> item = index.searchLargestSmallerThan(rid);
    if (item == null) {
      throw new IOException("Cannot find the column file containing " + rid);
    }
    String columnFilePath = item.getValue();

    if (scanner != null &&
        columnFilePath.equals(scanner.getPath().getName())) {
      scanner.pos(rid);
    } else {
      if (scanner != null) {
        scanner.close();
        scanner = null;
      }

      short i;
      for (i = 0; i < dataPaths.length; i++) {
        if (dataPaths[i].getName().equals(columnFilePath)) {
          next = i;
          break;
        }
      }

      if (i == dataPaths.length) {
        throw new IOException(columnFilePath + " is not data path");
      } else  {
        try {
          scanner = new Scanner(conf, dataPaths[next++]);
          scanner.pos(rid);
        } catch (UnknownDataTypeException e) {
          throw new IOException(e);
        } catch (UnknownCodecException e) {
          throw new IOException(e);
        }
      }
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
