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
import org.apache.tajo.error.Errors;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;

public class InvalidCastException extends TajoException {
	private static final long serialVersionUID = -7689027447969916148L;

	public InvalidCastException(ReturnState state) {
		super(state);
	}

	public InvalidCastException(TajoDataTypes.DataType src, TajoDataTypes.DataType target) {
		super(Errors.ResultCode.INVALID_CAST, src.getType().name(), target.getType().name());
	}

  public InvalidCastException(TajoDataTypes.Type src, TajoDataTypes.Type target) {
    super(Errors.ResultCode.INVALID_CAST, src.name(), target.name());
  }
}
