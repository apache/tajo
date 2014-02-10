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

package org.apache.tajo.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.tajo.QueryId;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.FileScanner;
import org.apache.tajo.storage.MergeScanner;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.FileFragment;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class TajoResultSet extends TajoResultSetBase {
  private FileSystem fs;
  private Scanner scanner;
  private TajoClient tajoClient;
  QueryId queryId;

  public TajoResultSet(TajoClient tajoClient, QueryId queryId) {
    this.tajoClient = tajoClient;
    this.queryId = queryId;
    init();
  }

  public TajoResultSet(TajoClient tajoClient, QueryId queryId,
                       TajoConf conf, TableDesc desc) throws IOException {
    this.tajoClient = tajoClient;
    this.queryId = queryId;
    if(desc != null) {
      this.schema = desc.getSchema();

      fs = FileScanner.getFileSystem(conf, desc.getPath());
      this.totalRow = desc.getStats() != null ? desc.getStats().getNumRows() : 0;

      List<FileFragment> frags = getFragments(desc.getPath());
      scanner = new MergeScanner(conf, schema, desc.getMeta(), frags);
    }
    init();
  }

  @Override
  protected void init() {
    cur = null;
    curRow = 0;
  }

  public static class FileNameComparator implements Comparator<FileStatus> {

    @Override
    public int compare(FileStatus f1, FileStatus f2) {
      return f1.getPath().getName().compareTo(f2.getPath().getName());
    }
  }

  private List<FileFragment> getFragments(Path tablePath)
      throws IOException {
    List<FileFragment> fragments = Lists.newArrayList();
    FileStatus[] files = fs.listStatus(tablePath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().charAt(0) != '.';
      }
    });


    // The files must be sorted in an ascending order of file names
    // in order to guarantee the order of a sort operation.
    // This is because our distributed sort algorithm outputs
    // a sequence of sorted data files, each of which contains sorted rows
    // within each file.
    Arrays.sort(files, new FileNameComparator());

    String tbname = tablePath.getName();
    for (int i = 0; i < files.length; i++) {
      if (files[i].getLen() == 0) {
        continue;
      }
      fragments.add(new FileFragment(tbname + "_" + i, files[i].getPath(), 0l, files[i].getLen()));
    }
    return ImmutableList.copyOf(fragments);
  }

  @Override
  public void close() throws SQLException {
    try {
      if(tajoClient != null) {
        this.tajoClient.closeQuery(queryId);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    try {
      if(scanner != null) {
        this.scanner.close();
      }
      //TODO clean temp result file
      cur = null;
      curRow = -1;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void beforeFirst() throws SQLException {
    try {
      if(scanner != null) {
        scanner.reset();
      }
      init();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  @Override
  protected Tuple nextTuple() throws IOException {
    if(scanner == null) {
      return null;
    }
    return scanner.next();
  }

  public boolean hasResult() {
    return scanner != null;
  }
}
