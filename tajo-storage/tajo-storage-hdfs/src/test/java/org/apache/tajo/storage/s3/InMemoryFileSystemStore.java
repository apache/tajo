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

package org.apache.tajo.storage.s3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.Block;
import org.apache.hadoop.fs.s3.INode;
import org.apache.hadoop.fs.s3.FileSystemStore;
import org.apache.hadoop.fs.s3.S3FileSystem;
import org.apache.tajo.common.exception.NotImplementedException;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * A stub implementation of {@link FileSystemStore} for testing
 * {@link S3FileSystem} without actually connecting to S3.
 */
public class InMemoryFileSystemStore implements FileSystemStore {

  private Configuration conf;
  private SortedMap<Path, INode> inodes = new TreeMap<Path, INode>();
  private Map<Long, byte[]> blocks = new HashMap<Long, byte[]>();

  @Override
  public void initialize(URI uri, Configuration conf) {
    this.conf = conf;
  }

  @Override
  public String getVersion() throws IOException {
    return "0";
  }

  @Override
  public void deleteINode(Path path) throws IOException {
    inodes.remove(normalize(path));
  }

  @Override
  public void deleteBlock(Block block) throws IOException {
    blocks.remove(block.getId());
  }

  @Override
  public boolean inodeExists(Path path) throws IOException {
    return inodes.containsKey(normalize(path));
  }

  @Override
  public boolean blockExists(long blockId) throws IOException {
    return blocks.containsKey(blockId);
  }

  @Override
  public INode retrieveINode(Path path) throws IOException {
    return inodes.get(normalize(path));
  }

  @Override
  public File retrieveBlock(Block block, long byteRangeStart) throws IOException {
    byte[] data = blocks.get(block.getId());
    File file = createTempFile();
    BufferedOutputStream out = null;
    try {
      out = new BufferedOutputStream(new FileOutputStream(file));
      out.write(data, (int) byteRangeStart, data.length - (int) byteRangeStart);
    } finally {
      if (out != null) {
        out.close();
      }
    }
    return file;
  }

  private File createTempFile() throws IOException {
    File dir = new File(conf.get("fs.s3.buffer.dir"));
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Cannot create S3 buffer directory: " + dir);
    }
    File result = File.createTempFile("test-", ".tmp", dir);
    result.deleteOnExit();
    return result;
  }

  @Override
  public Set<Path> listSubPaths(Path path) throws IOException {
    Path normalizedPath = normalize(path);
    // This is inefficient but more than adequate for testing purposes.
    Set<Path> subPaths = new LinkedHashSet<Path>();
    for (Path p : inodes.tailMap(normalizedPath).keySet()) {
      if (normalizedPath.equals(p.getParent())) {
        subPaths.add(p);
      }
    }
    return subPaths;
  }

  @Override
  public Set<Path> listDeepSubPaths(Path path) throws IOException {
    Path normalizedPath = normalize(path);
    String pathString = normalizedPath.toUri().getPath();
    if (!pathString.endsWith("/")) {
      pathString += "/";
    }
    // This is inefficient but more than adequate for testing purposes.
    Set<Path> subPaths = new LinkedHashSet<Path>();
    for (Path p : inodes.tailMap(normalizedPath).keySet()) {
      if (p.toUri().getPath().startsWith(pathString)) {
        subPaths.add(p);
      }
    }
    return subPaths;
  }

  @Override
  public void storeINode(Path path, INode inode) throws IOException {
    inodes.put(normalize(path), inode);
  }

  @Override
  public void storeBlock(Block block, File file) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int numRead;
    BufferedInputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(file));
      while ((numRead = in.read(buf)) >= 0) {
        out.write(buf, 0, numRead);
      }
    } finally {
      if (in != null) {
        in.close();
      }
    }
    blocks.put(block.getId(), out.toByteArray());
  }

  private Path normalize(Path path) {
    if (!path.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }
    return new Path(path.toUri().getPath());
  }

  @Override
  public void purge() throws IOException {
    inodes.clear();
    blocks.clear();
  }

  @Override
  public void dump() throws IOException {
    throw new NotImplementedException();
  }

}
