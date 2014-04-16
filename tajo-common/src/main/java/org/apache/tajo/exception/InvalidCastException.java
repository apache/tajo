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

package org.apache.tajo.exception;

import org.apache.tajo.common.TajoDataTypes;

public class InvalidCastException extends RuntimeException {
	private static final long serialVersionUID = -7689027447969916148L;

	public InvalidCastException() {
	}

	public InvalidCastException(TajoDataTypes.DataType src, TajoDataTypes.DataType target) {
		super(src.getType().name() + " value cannot be casted to " + target.getType().name());
	}

  public InvalidCastException(TajoDataTypes.Type src, TajoDataTypes.Type target) {
    super(src.name() + " value cannot be casted to " + target.name());
  }
}
