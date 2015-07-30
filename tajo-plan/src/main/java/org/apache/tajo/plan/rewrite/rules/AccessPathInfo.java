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

package org.apache.tajo.plan.rewrite.rules;

import org.apache.tajo.catalog.statistics.TableStats;

public abstract class AccessPathInfo {
  public enum ScanTypeControl {
    INDEX_SCAN,
    SEQ_SCAN
  }

  private ScanTypeControl scanType;
  private TableStats tableStats;

  public AccessPathInfo(ScanTypeControl scanType, TableStats tableStats) {
    this.scanType = scanType;
    this.tableStats = tableStats;
  }

  public ScanTypeControl getScanType() {
    return scanType;
  }

  public TableStats getTableStats() {
    return tableStats;
  }

  public void setTableStats(TableStats tableStats) {
    this.tableStats = tableStats;
  }

  public boolean hasTableStats() {
    return this.tableStats != null;
  }
}
