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

package org.apache.tajo.worker;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

public class TaskHistory {
  private long startTime;
  private long finishTime;

  private String status;
  private String outputPath;
  private String workingPath;
  private float progress;

  Map<URI, FetcherHistory> fetchers;

  public static class FetcherHistory {
    private long startTime;
    private long finishTime;

    private String status;
    private String uri;
    private long fileLen;

    public long getStartTime() {
      return startTime;
    }

    public void setStartTime(long startTime) {
      this.startTime = startTime;
    }

    public long getFinishTime() {
      return finishTime;
    }

    public void setFinishTime(long finishTime) {
      this.finishTime = finishTime;
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }

    public String getUri() {
      return uri;
    }

    public void setUri(String uri) {
      this.uri = uri;
    }

    public long getFileLen() {
      return fileLen;
    }

    public void setFileLen(long fileLen) {
      this.fileLen = fileLen;
    }
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }

  public String getWorkingPath() {
    return workingPath;
  }

  public void setWorkingPath(String workingPath) {
    this.workingPath = workingPath;
  }

  public Collection<FetcherHistory> getFetchers() {
    return fetchers.values();
  }

  public void setFetchers(Map<URI, FetcherHistory> fetchers) {
    this.fetchers = fetchers;
  }

  public float getProgress() {
    return progress;
  }

  public void setProgress(float progress) {
    this.progress = progress;
  }

  public boolean hasFetcher() {
    return fetchers != null && !fetchers.isEmpty();
  }
}
