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

package org.apache.tajo.storage;

/**
 * Format properties
 */
public class FormatProperty {

  /** if this format supports insert operation */
  private boolean insertable;
  /** if this format supports direct insertion (e.g., HBASE or JDBC-based storages) */
  private boolean directInsert;
  /** if this format supports staging phase */
  private boolean stagingSupport;

  public FormatProperty(boolean insertable, boolean directInsert, boolean stagingSupport) {
    this.insertable = insertable;
    this.stagingSupport = stagingSupport;
    this.directInsert = directInsert;
  }

  /**
   * Return if this format supports staging phase
   * @return True if this format supports staging phase
   */
  public boolean isInsertable() {
    return insertable;
  }

  /**
   * Return if this format supports direct insertion (e.g., HBASE or JDBC-based storages)
   * @return True if this format supports direct insertion
   */
  public boolean directInsertSupported() {
    return directInsert;
  }

  /**
   * Return if this format supports staging phase
   *
   * @return True if this format supports staging phase
   */
  public boolean isStagingSupport() {
    return stagingSupport;
  }
}
