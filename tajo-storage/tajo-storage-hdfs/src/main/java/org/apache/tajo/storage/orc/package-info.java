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
 * Provides read and write support for ORC files. Tajo schemas are
 * converted to ORC struct type
 * </p>
 *
 * <table>
 *   <tr>
 *     <th>Tajo type</th>
 *     <th>ORC type</th>
 *   </tr>
 *   <tr>
 *     <td>NULL_TYPE</td>
 *     <td>BOOLEAN, but all fields are marked as null</td>
 *   </tr>
 *   <tr>
 *     <td>BOOLEAN</td>
 *     <td>BOOLEAN</td>
 *   </tr>
 *   <tr>
 *     <td>BYTE</td>
 *     <td>BYTE</td>
 *   </tr>
 *   <tr>
 *     <td>INT2</td>
 *     <td>SHORT</td>
 *   </tr>
 *   <tr>
 *     <td>INT4</td>
 *     <td>INTEGER</td>
 *   </tr>
 *   <tr>
 *     <td>INT8</td>
 *     <td>LONG</td>
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
 *     <td>CHAR/TEXT</td>
 *     <td>STRING</td>
 *   </tr>
 *   <tr>
 *     <td>BLOB/PROTOBUF</td>
 *     <td>BINARY</td>
 *   </tr>
 *   <tr>
 *     <td>INET4</td>
 *     <td>INTEGER</td>
 *   </tr>
 *   <tr>
 *     <td>TIMESTAMP</td>
 *     <td>TIMESTAMP</td>
 *   </tr>
 *   <tr>
 *     <td>DATE</td>
 *     <td>DATE</td>
 *   </tr>
 * </table>
 *
 * <p>
 * Because Tajo fields can be NULL, all ORC fields are marked as optional.
 * </p>
 *
 * <p>
 * The conversion from Tajo to ORC is lossy without the original Tajo
 * schema. As a result, ORC files are read using the Tajo schema saved in
 * the Tajo catalog for the table the ORC files belong to, which was
 * defined when the table was created.
 * </p>
 */

package org.apache.tajo.storage.orc;
