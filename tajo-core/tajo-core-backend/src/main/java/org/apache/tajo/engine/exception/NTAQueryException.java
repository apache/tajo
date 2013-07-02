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
 * 
 */
package org.apache.tajo.engine.exception;

import java.io.IOException;

public class NTAQueryException extends IOException {
	private static final long serialVersionUID = -5012296598261064705L;

	public NTAQueryException() {
	}
	
	public NTAQueryException(Exception e) {
		super(e);
	}

	/**
	 * @param query
	 */
	public NTAQueryException(String query) {
		super(query);
	}
}
