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

package org.apache.tajo.util.history;

import com.google.gson.annotations.Expose;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.json.GsonObject;

public class QueryUnitHistory implements GsonObject {
  @Expose private String id;
  @Expose private String hostAndPort;
  @Expose private int httpPort;
  @Expose private String state;
  @Expose private float progress;
  @Expose private long launchTime;
  @Expose private long finishTime;
  @Expose private int retryCount;

  @Expose private int numShuffles;
  @Expose private String shuffleKey;
  @Expose private String shuffleFileName;

  @Expose private String[] dataLocations;
  @Expose private String[] fragments;
  @Expose private String[][] fetchs;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getHostAndPort() {
    return hostAndPort;
  }

  public void setHostAndPort(String hostAndPort) {
    this.hostAndPort = hostAndPort;
  }

  public int getHttpPort() {
    return httpPort;
  }

  public void setHttpPort(int httpPort) {
    this.httpPort = httpPort;
  }

  public int getRetryCount() {
    return retryCount;
  }

  public void setRetryCount(int retryCount) {
    this.retryCount = retryCount;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public float getProgress() {
    return progress;
  }

  public void setProgress(float progress) {
    this.progress = progress;
  }

  public long getLaunchTime() {
    return launchTime;
  }

  public void setLaunchTime(long launchTime) {
    this.launchTime = launchTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public int getNumShuffles() {
    return numShuffles;
  }

  public void setNumShuffles(int numShuffles) {
    this.numShuffles = numShuffles;
  }

  public String getShuffleKey() {
    return shuffleKey;
  }

  public void setShuffleKey(String shuffleKey) {
    this.shuffleKey = shuffleKey;
  }

  public String getShuffleFileName() {
    return shuffleFileName;
  }

  public void setShuffleFileName(String shuffleFileName) {
    this.shuffleFileName = shuffleFileName;
  }

  public String[] getDataLocations() {
    return dataLocations;
  }

  public void setDataLocations(String[] dataLocations) {
    this.dataLocations = dataLocations;
  }

  public String[] getFragments() {
    return fragments;
  }

  public void setFragments(String[] fragments) {
    this.fragments = fragments;
  }

  public String[][] getFetchs() {
    return fetchs;
  }

  public void setFetchs(String[][] fetchs) {
    this.fetchs = fetchs;
  }

  @Override
  public String toJson() {
    return CoreGsonHelper.toJson(this, QueryUnitHistory.class);
  }

  public long getRunningTime() {
    if(finishTime > 0) {
      return finishTime - launchTime;
    } else {
      return 0;
    }
  }
}
