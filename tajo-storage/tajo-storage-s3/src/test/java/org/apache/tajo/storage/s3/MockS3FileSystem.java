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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;

public class MockS3FileSystem extends FileSystem {
  private URI uri;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);

    setConf(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
  }


  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>s3</code>
   */
  @Override
  public String getScheme() {
    return "s3";
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public Path makeQualified(Path path) {
    return path;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return null;
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
    short replication, long blockSize, Progressable progress) throws IOException {
    return null;
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    return null;
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return false;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return false;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return new FileStatus[0];
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
  }

  @Override
  public Path getWorkingDirectory() {
    return null;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return false;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return null;
  }
}
