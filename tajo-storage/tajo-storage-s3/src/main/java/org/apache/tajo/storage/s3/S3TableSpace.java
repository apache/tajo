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

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.QueryVars;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.storage.*;

import net.minidev.json.JSONObject;

public class S3TableSpace extends FileTablespace {
  private static final StorageProperty s3StorageProperties = new StorageProperty("TEXT", false, true, true, false);

  public S3TableSpace(String spaceName, URI uri, JSONObject config) {
    super(spaceName, uri, config);
  }

  @Override
  public StorageProperty getProperty() {
    return s3StorageProperties;
  }

  @Override
  public URI getStagingUri(OverridableConf context, String queryId, TableMeta meta) throws IOException {
    if (!context.get(QueryVars.OUTPUT_TABLE_URI, "").isEmpty()) {
      String outputPath = stagingRootPath.toString();

      URI stagingRootUri = stagingRootPath.toUri();

      outputPath = outputPath.replaceAll(stagingRootUri.getScheme() + "://" + stagingRootUri.getHost(), "file://");

      FileSystem localFileSystem = FileSystem.getLocal(conf);
      Path stagingDir = localFileSystem.makeQualified(StorageUtil.concatPath(outputPath, TMP_STAGING_DIR_PREFIX,
        queryId));

      return stagingDir.toUri();
    } else {
      return super.getStagingUri(context, queryId, meta);
    }
  }

  @Override
  protected boolean commitTask(Path sourcePath, Path targetPath) throws IOException {
    try {
      FileSystem localFileSystem = FileSystem.getLocal(conf);
      if (localFileSystem.isDirectory(sourcePath)) {
        FileStatus[] statuses = localFileSystem.listStatus(sourcePath);
        for(FileStatus status: statuses) {
          this.fs.copyFromLocalFile(status.getPath(), targetPath);
        }
      } else {
        this.fs.copyFromLocalFile(sourcePath, targetPath);
      }
      return true;
    } catch (IOException e) {
      throw new IOException("Failed to commit Task - source:" + sourcePath + ", target:" + targetPath, e);
    }
  }
  
}