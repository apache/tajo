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
/**
 * <p>
 * Provides read and write support for Parquet files. Tajo schemas are
 * converted to Parquet schemas according to the following mapping of Tajo
 * and Parquet types:
 * </p>
 *
 * <table>
 *   <tr>
 *     <th>Tajo type</th>
 *     <th>Parquet type</th>
 *   </tr>
 *   <tr>
 *     <td>NULL_TYPE</td>
 *     <td>No type. The field is not encoded in Parquet.</td>
 *   </tr>
 *   <tr>
 *     <td>BOOLEAN</td>
 *     <td>BOOLEAN</td>
 *   </tr>
 *   <tr>
 *     <td>BIT</td>
 *     <td>INT32</td>
 *   </tr>
 *   <tr>
 *     <td>INT2</td>
 *     <td>INT32</td>
 *   </tr>
 *   <tr>
 *     <td>INT4</td>
 *     <td>INT32</td>
 *   </tr>
 *   <tr>
 *     <td>INT8</td>
 *     <td>INT64</td>
 *   </tr>
 *   <tr>
 *     <td>FLOAT4</td>
 *     <td>FLOAT</td>
 *   </tr>
 *   <tr>
 *     <td>FLOAT8</td>
 *     <td>DOUBLE</td>
 *   </tr>
 *   <tr>
 *     <td>CHAR</td>
 *     <td>BINARY (with OriginalType UTF8)</td>
 *   </tr>
 *   <tr>
 *     <td>TEXT</td>
 *     <td>BINARY (with OriginalType UTF8)</td>
 *   </tr>
 *   <tr>
 *     <td>PROTOBUF</td>
 *     <td>BINARY</td>
 *   </tr>
 *   <tr>
 *     <td>BLOB</td>
 *     <td>BINARY</td>
 *   </tr>
 * </table>
 *
 * <p>
 * Because Tajo fields can be NULL, all Parquet fields are marked as optional.
 * </p>
 *
 * <p>
 * The conversion from Tajo to Parquet is lossy without the original Tajo
 * schema. As a result, Parquet files are read using the Tajo schema saved in
 * the Tajo catalog for the table the Parquet files belong to, which was
 * defined when the table was created.
 * </p>
 */

package org.apache.tajo.storage.parquet;
