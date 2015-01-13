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

package org.apache.tajo.unit;

public class StorageUnit {

	public static final int B = 8;
	public static final int KB = 1024;
	public static final int MB = KB * KB;
	public static final int GB = MB * KB;
	// To prevent a overflow in a integer variable, this variable size increased to 64bit size.
	public static final long TB = (long)GB * KB;
	public static final long PB = TB * KB;
}
