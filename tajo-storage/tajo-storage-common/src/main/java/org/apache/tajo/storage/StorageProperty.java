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

/**
 * Storage Properties
 */
public class StorageProperty {
  /** default file format */
  private final String defaultFormat;
  /** if this storage supports move operator */
  private final boolean movable;
  /** if this storage supports is writable */
  private final boolean writable;
  /** if this storage allows use of artibrary paths */
  private final boolean absolutePathAllowed;
  /** if this storage provides metadata provider */
  private final boolean metadataProvided;

  public StorageProperty(String defaultFormat,
                         boolean movable,
                         boolean writable,
                         boolean absolutePathAllowed,
                         boolean metadataProvided) {

    this.defaultFormat = defaultFormat;
    this.movable = movable;
    this.writable = writable;
    this.absolutePathAllowed = absolutePathAllowed;
    this.metadataProvided = metadataProvided;
  }

  /**
   * Return default file format
   * @return Default file format
   */
  public String defaultFormat() {
    return defaultFormat;
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
   * Does this storage allows the use of arbitrary absolute paths outside tablespace?
   *
   * @return True if this storage allows accesses to artibrary paths.
   */
  public boolean isArbitraryPathAllowed() {
    return this.absolutePathAllowed;
  }

  /**
   * Is metadata provided?
   *
   * @return True if this storage provides linked metadata.
   */
  public boolean isMetadataProvided() {
    return this.metadataProvided;
  }
}
