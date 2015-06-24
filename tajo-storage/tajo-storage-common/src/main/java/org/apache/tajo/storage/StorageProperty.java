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

package org.apache.tajo.storage;

public class StorageProperty {
  private boolean movable;
  private boolean writable;
  private boolean insertable;
  private boolean absolutePathAllowed;

  public StorageProperty(boolean movable, boolean writable, boolean isInsertable, boolean absolutePathAllowed) {
    this.movable = movable;
    this.writable = writable;
    this.insertable = isInsertable;
    this.absolutePathAllowed = absolutePathAllowed;
  }

  /**
   * Move-like operation is allowed
   *
   * @return true if move operation is available
   */
  public boolean isMovable() {
    return movable;
  }

  /**
   * Is it Writable storage?
   *
   * @return true if this storage is writable.
   */
  public boolean isWritable() {
    return writable;
  }

  /**
   * this storage supports insert operation?
   *
   * @return true if insert operation is allowed.
   */
  public boolean isInsertable() {
    return insertable;
  }

  /**
   * Does this storage allows the use of arbitrary absolute paths outside tablespace?
   *
   * @return
   */
  public boolean isArbitraryPathAllowed() {
    return this.absolutePathAllowed;
  }
}
