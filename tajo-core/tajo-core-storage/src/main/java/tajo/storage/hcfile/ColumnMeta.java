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

package tajo.storage.hcfile;

import tajo.catalog.proto.CatalogProtos.CompressType;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;

public interface ColumnMeta {

  StoreType getStoreType();

  /**
   * Return the type of this column
   *
   * @return the type of this column
   */
  DataType getDataType();

  CompressType getCompressType();

  /**
   * Indicating this column is compressed or not
   *
   * @return return true if this column is compressed, otherwise return false
   */
  boolean isCompressed();

  boolean isSorted();

  boolean isContiguous();
}
