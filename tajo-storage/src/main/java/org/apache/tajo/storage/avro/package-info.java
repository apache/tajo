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
 * Provides read and write support for Avro files. Avro schemas are
 * converted to Tajo schemas according to the following mapping of Avro
 * and Tajo types:
 * </p>
 *
 * <table>
 *   <tr>
 *     <th>Avro type</th>
 *     <th>Tajo type</th>
 *   </tr>
 *   <tr>
 *     <td>NULL</td>
 *     <td>NULL_TYPE</td>
 *   </tr>
 *   <tr>
 *     <td>BOOLEAN</td>
 *     <td>BOOLEAN</td>
 *   </tr>
 *   <tr>
 *     <td>INT</td>
 *     <td>INT4</td>
 *   </tr>
 *   <tr>
 *     <td>LONG</td>
 *     <td>INT8</td>
 *   </tr>
 *   <tr>
 *     <td>FLOAT</td>
 *     <td>FLOAT4</td>
 *   </tr>
 *   <tr>
 *     <td>DOUBLE</td>
 *     <td>FLOAT8</td>
 *   </tr>
 *   <tr>
 *     <td>BYTES</td>
 *     <td>BLOB</td>
 *   </tr>
 *   <tr>
 *     <td>STRING</td>
 *     <td>TEXT</td>
 *   </tr>
 *   <tr>
 *     <td>FIXED</td>
 *     <td>BLOB</td>
 *   </tr>
 *   <tr>
 *     <td>RECORD</td>
 *     <td>Not currently supported</td>
 *   </tr>
 *   <tr>
 *     <td>ENUM</td>
 *     <td>Not currently supported.</td>
 *   </tr>
 *   <tr>
 *     <td>MAP</td>
 *     <td>Not currently supported.</td>
 *   </tr>
 *   <tr>
 *     <td>UNION</td>
 *     <td>Not currently supported.</td>
 *   </tr>
 * </table>
 */

package org.apache.tajo.storage.avro;
