/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.plan.verifier.VerificationState;
import org.junit.Test;

import static org.apache.tajo.catalog.proto.CatalogProtos.StoreType.JSON;
import static org.apache.tajo.catalog.proto.CatalogProtos.StoreType.PARQUET;
import static org.junit.Assert.assertEquals;

public class TestDataTypeValidator {
  @Test
  public void testParquetExpectOk() throws Exception{
    VerificationState state = new VerificationState();
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("age", Type.INT2);

    DataTypeValidatorFactory.validate(state, PARQUET, schema);
    assertEquals(state.getErrorMessages().size(), 0);
  }

  @Test
  public void testParquetExpectException() {
    VerificationState state = new VerificationState();

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("age", Type.INT2);
    schema.addColumn("mytime", Type.DATE);

    DataTypeValidatorFactory.validate(state, PARQUET, schema);
    assertEquals(state.getErrorMessages().size(), 1);
  }

  @Test
  public void testJsonExpectOk() throws Exception{
    VerificationState state = new VerificationState();

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("age", Type.INT2);

    DataTypeValidatorFactory.validate(state, JSON, schema);
    assertEquals(state.getErrorMessages().size(), 0);
  }

  @Test
  public void testJsonExpectException() {
    VerificationState state = new VerificationState();

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("age", Type.INT2);
    schema.addColumn("myproto", Type.PROTOBUF);

    DataTypeValidatorFactory.validate(state, JSON, schema);
    assertEquals(state.getErrorMessages().size(), 1);
  }

  @Test
  public void testJsonExpectException2() {
    VerificationState state = new VerificationState();

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("age", Type.INT2);
    schema.addColumn("myproto", Type.PROTOBUF);
    schema.addColumn("myproto2", Type.PROTOBUF);

    DataTypeValidatorFactory.validate(state, JSON, schema);
    assertEquals(state.getErrorMessages().size(), 2);
  }
}
