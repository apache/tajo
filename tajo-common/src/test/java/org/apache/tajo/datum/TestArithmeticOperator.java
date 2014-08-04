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

package org.apache.tajo.datum;

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TestArithmeticOperator {
  String option;

  public TestArithmeticOperator(String option) {
    this.option = option;
  }

  @Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][]{
        {"Zero_Null"},
        {"Zero_Exception"},
    });
  }

  @Before
  public void setUp() {
    TajoConf tajoConf = new TajoConf();
    if ("Zero_Exception".equals(option)) {
      tajoConf.setBoolVar(ConfVars.BEHAVIOR_ARITHMETIC_ABORT, true);
    } else {
      tajoConf.setBoolVar(ConfVars.BEHAVIOR_ARITHMETIC_ABORT, false);
    }
    Datum.initAbortWhenDivideByZero(tajoConf);
  }

  @Test
  public void testInt2Datum() throws Exception {
    //plus
    runAndAssert("plus", new Int2Datum((short)10), new Int2Datum((short)5), new Int2Datum((short)15));
    runAndAssert("plus", new Int2Datum((short)10), new Int4Datum(5), new Int4Datum(15));
    runAndAssert("plus", new Int2Datum((short)10), new Int8Datum(5), new Int8Datum(15));
    runAndAssert("plus", new Int2Datum((short)10), new Float4Datum(5.0f), new Float4Datum(15.0f));
    runAndAssert("plus", new Int2Datum((short)10), new Float8Datum(5.0), new Float8Datum(15.0));

    //minus
    runAndAssert("minus", new Int2Datum((short)10), new Int2Datum((short)5), new Int2Datum((short)5));
    runAndAssert("minus", new Int2Datum((short)10), new Int4Datum(5), new Int4Datum(5));
    runAndAssert("minus", new Int2Datum((short)10), new Int8Datum(5), new Int8Datum(5));
    runAndAssert("minus", new Int2Datum((short)10), new Float4Datum(5.0f), new Float4Datum(5.0f));
    runAndAssert("minus", new Int2Datum((short)10), new Float8Datum(5.0), new Float8Datum(5.0));

    runAndAssert("minus", new Int2Datum((short)5), new Int2Datum((short)10), new Int2Datum((short)-5));
    runAndAssert("minus", new Int2Datum((short)5), new Int4Datum(10), new Int4Datum(-5));
    runAndAssert("minus", new Int2Datum((short)5), new Int8Datum(10), new Int8Datum(-5));
    runAndAssert("minus", new Int2Datum((short)5), new Float4Datum(10.0f), new Float4Datum(-5.0f));
    runAndAssert("minus", new Int2Datum((short)5), new Float8Datum(10.0), new Float8Datum(-5.0));

    //multiply
    runAndAssert("multiply", new Int2Datum((short)10), new Int2Datum((short)5), new Int4Datum(50));
    runAndAssert("multiply", new Int2Datum((short)10), new Int4Datum(5), new Int4Datum(50));
    runAndAssert("multiply", new Int2Datum((short)10), new Int8Datum(5), new Int8Datum(50));
    runAndAssert("multiply", new Int2Datum((short)10), new Float4Datum(5.0f), new Float4Datum(50.0f));
    runAndAssert("multiply", new Int2Datum((short)10), new Float8Datum(5.0), new Float8Datum(50.0));

    //divide
    runAndAssert("divide", new Int2Datum((short)10), new Int2Datum((short)5), new Int2Datum((short)2));
    runAndAssert("divide", new Int2Datum((short)10), new Int4Datum(5), new Int4Datum(2));
    runAndAssert("divide", new Int2Datum((short)10), new Int8Datum(5), new Int8Datum(2));
    runAndAssert("divide", new Int2Datum((short)10), new Float4Datum(5.0f), new Float4Datum(2.0f));
    runAndAssert("divide", new Int2Datum((short)10), new Float8Datum(5.0), new Float8Datum(2.0));

    runAndAssert("divide", new Int2Datum((short)10), new Int2Datum((short) 0), NullDatum.get());
    runAndAssert("divide", new Int2Datum((short)10), new Int4Datum(0), NullDatum.get());
    runAndAssert("divide", new Int2Datum((short)10), new Int8Datum(0), NullDatum.get());
    runAndAssert("divide", new Int2Datum((short)10), new Float4Datum(0.0f), NullDatum.get());
    runAndAssert("divide", new Int2Datum((short)10), new Float8Datum(0.0), NullDatum.get());

    //modular
    runAndAssert("modular", new Int2Datum((short)10), new Int2Datum((short)3), new Int2Datum((short)1));
    runAndAssert("modular", new Int2Datum((short)10), new Int4Datum(3), new Int4Datum(1));
    runAndAssert("modular", new Int2Datum((short)10), new Int8Datum(3), new Int8Datum(1));
    runAndAssert("modular", new Int2Datum((short)10), new Float4Datum(3.0f), new Float4Datum(1.0f));
    runAndAssert("modular", new Int2Datum((short)10), new Float8Datum(3.0), new Float8Datum(1.0));

    runAndAssert("modular", new Int2Datum((short)10), new Int2Datum((short) 0), NullDatum.get());
    runAndAssert("modular", new Int2Datum((short)10), new Int4Datum(0), NullDatum.get());
    runAndAssert("modular", new Int2Datum((short)10), new Int8Datum(0), NullDatum.get());
    runAndAssert("modular", new Int2Datum((short)10), new Float4Datum(0.0f), NullDatum.get());
    runAndAssert("modular", new Int2Datum((short)10), new Float8Datum(0.0), NullDatum.get());
  }

  @Test
  public void testInt4Datum() throws Exception {
    //plus
    runAndAssert("plus", new Int4Datum(10), new Int2Datum((short)5), new Int4Datum(15));
    runAndAssert("plus", new Int4Datum(10), new Int4Datum(5), new Int4Datum(15));
    runAndAssert("plus", new Int4Datum(10), new Int8Datum(5), new Int8Datum(15));
    runAndAssert("plus", new Int4Datum(10), new Float4Datum(5.0f), new Float4Datum(15.0f));
    runAndAssert("plus", new Int4Datum(10), new Float8Datum(5.0), new Float8Datum(15.0));

    //minus
    runAndAssert("minus", new Int4Datum(10), new Int2Datum((short)5), new Int4Datum(5));
    runAndAssert("minus", new Int4Datum(10), new Int4Datum(5), new Int4Datum(5));
    runAndAssert("minus", new Int4Datum(10), new Int8Datum(5), new Int8Datum(5));
    runAndAssert("minus", new Int4Datum(10), new Float4Datum(5.0f), new Float4Datum(5.0f));
    runAndAssert("minus", new Int4Datum(10), new Float8Datum(5.0), new Float8Datum(5.0));

    runAndAssert("minus", new Int4Datum(5), new Int2Datum((short)10), new Int4Datum(-5));
    runAndAssert("minus", new Int4Datum(5), new Int4Datum(10), new Int4Datum(-5));
    runAndAssert("minus", new Int4Datum(5), new Int8Datum(10), new Int8Datum(-5));
    runAndAssert("minus", new Int4Datum(5), new Float4Datum(10.0f), new Float4Datum(-5.0f));
    runAndAssert("minus", new Int4Datum(5), new Float8Datum(10.0), new Float8Datum(-5.0));

    //multiply
    runAndAssert("multiply", new Int4Datum(10), new Int2Datum((short)5), new Int4Datum(50));
    runAndAssert("multiply", new Int4Datum(10), new Int4Datum(5), new Int4Datum(50));
    runAndAssert("multiply", new Int4Datum(10), new Int8Datum(5), new Int8Datum(50));
    runAndAssert("multiply", new Int4Datum(10), new Float4Datum(5.0f), new Float4Datum(50.0f));
    runAndAssert("multiply", new Int4Datum(10), new Float8Datum(5.0), new Float8Datum(50.0));

    //divide
    runAndAssert("divide", new Int4Datum(10), new Int2Datum((short)5), new Int4Datum(2));
    runAndAssert("divide", new Int4Datum(10), new Int4Datum(5), new Int4Datum(2));
    runAndAssert("divide", new Int4Datum(10), new Int8Datum(5), new Int8Datum(2));
    runAndAssert("divide", new Int4Datum(10), new Float4Datum(5.0f), new Float4Datum(2.0f));
    runAndAssert("divide", new Int4Datum(10), new Float8Datum(5.0), new Float8Datum(2.0));

    runAndAssert("divide", new Int4Datum(10), new Int2Datum((short) 0), NullDatum.get());
    runAndAssert("divide", new Int4Datum(10), new Int4Datum(0), NullDatum.get());
    runAndAssert("divide", new Int4Datum(10), new Int8Datum(0), NullDatum.get());
    runAndAssert("divide", new Int4Datum(10), new Float4Datum(0.0f), NullDatum.get());
    runAndAssert("divide", new Int4Datum(10), new Float8Datum(0.0), NullDatum.get());

    //modular
    runAndAssert("modular", new Int4Datum(10), new Int2Datum((short)3), new Int4Datum(1));
    runAndAssert("modular", new Int4Datum(10), new Int4Datum(3), new Int4Datum(1));
    runAndAssert("modular", new Int4Datum(10), new Int8Datum(3), new Int8Datum(1));
    runAndAssert("modular", new Int4Datum(10), new Float4Datum(3.0f), new Float4Datum(1.0f));
    runAndAssert("modular", new Int4Datum(10), new Float8Datum(3.0), new Float8Datum(1.0));

    runAndAssert("modular", new Int4Datum(10), new Int2Datum((short) 0), NullDatum.get());
    runAndAssert("modular", new Int4Datum(10), new Int4Datum(0), NullDatum.get());
    runAndAssert("modular", new Int4Datum(10), new Int8Datum(0), NullDatum.get());
    runAndAssert("modular", new Int4Datum(10), new Float4Datum(0.0f), NullDatum.get());
    runAndAssert("modular", new Int4Datum(10), new Float8Datum(0.0), NullDatum.get());
  }

  @Test
  public void testInt8Datum() throws Exception {
    //plus
    runAndAssert("plus", new Int8Datum(10), new Int2Datum((short)5), new Int8Datum(15));
    runAndAssert("plus", new Int8Datum(10), new Int4Datum(5), new Int8Datum(15));
    runAndAssert("plus", new Int8Datum(10), new Int8Datum(5), new Int8Datum(15));
    runAndAssert("plus", new Int8Datum(10), new Float4Datum(5.0f), new Float8Datum(15.0f));
    runAndAssert("plus", new Int8Datum(10), new Float8Datum(5.0), new Float8Datum(15.0));

    //minus
    runAndAssert("minus", new Int8Datum(10), new Int2Datum((short)5), new Int8Datum(5));
    runAndAssert("minus", new Int8Datum(10), new Int4Datum(5), new Int8Datum(5));
    runAndAssert("minus", new Int8Datum(10), new Int8Datum(5), new Int8Datum(5));
    runAndAssert("minus", new Int8Datum(10), new Float4Datum(5.0f), new Float8Datum(5.0f));
    runAndAssert("minus", new Int8Datum(10), new Float8Datum(5.0), new Float8Datum(5.0));

    runAndAssert("minus", new Int8Datum(5), new Int2Datum((short)10), new Int8Datum(-5));
    runAndAssert("minus", new Int8Datum(5), new Int4Datum(10), new Int8Datum(-5));
    runAndAssert("minus", new Int8Datum(5), new Int8Datum(10), new Int8Datum(-5));
    runAndAssert("minus", new Int8Datum(5), new Float4Datum(10.0f), new Float8Datum(-5.0f));
    runAndAssert("minus", new Int8Datum(5), new Float8Datum(10.0), new Float8Datum(-5.0));

    //multiply
    runAndAssert("multiply", new Int8Datum(10), new Int2Datum((short)5), new Int8Datum(50));
    runAndAssert("multiply", new Int8Datum(10), new Int4Datum(5), new Int8Datum(50));
    runAndAssert("multiply", new Int8Datum(10), new Int8Datum(5), new Int8Datum(50));
    runAndAssert("multiply", new Int8Datum(10), new Float4Datum(5.0f), new Float8Datum(50.0f));
    runAndAssert("multiply", new Int8Datum(10), new Float8Datum(5.0), new Float8Datum(50.0));

    //divide
    runAndAssert("divide", new Int8Datum(10), new Int2Datum((short)5), new Int8Datum(2));
    runAndAssert("divide", new Int8Datum(10), new Int4Datum(5), new Int8Datum(2));
    runAndAssert("divide", new Int8Datum(10), new Int8Datum(5), new Int8Datum(2));
    runAndAssert("divide", new Int8Datum(10), new Float4Datum(5.0f), new Float8Datum(2.0f));
    runAndAssert("divide", new Int8Datum(10), new Float8Datum(5.0), new Float8Datum(2.0));

    runAndAssert("divide", new Int8Datum(10), new Int2Datum((short) 0), NullDatum.get());
    runAndAssert("divide", new Int8Datum(10), new Int4Datum(0), NullDatum.get());
    runAndAssert("divide", new Int8Datum(10), new Int8Datum(0), NullDatum.get());
    runAndAssert("divide", new Int8Datum(10), new Float4Datum(0.0f), NullDatum.get());
    runAndAssert("divide", new Int8Datum(10), new Float8Datum(0.0), NullDatum.get());

    //modular
    runAndAssert("modular", new Int8Datum(10), new Int2Datum((short)3), new Int8Datum(1));
    runAndAssert("modular", new Int8Datum(10), new Int4Datum(3), new Int8Datum(1));
    runAndAssert("modular", new Int8Datum(10), new Int8Datum(3), new Int8Datum(1));
    runAndAssert("modular", new Int8Datum(10), new Float4Datum(3.0f), new Float8Datum(1.0f));
    runAndAssert("modular", new Int8Datum(10), new Float8Datum(3.0), new Float8Datum(1.0));

    runAndAssert("modular", new Int8Datum(10), new Int2Datum((short) 0), NullDatum.get());
    runAndAssert("modular", new Int8Datum(10), new Int4Datum(0), NullDatum.get());
    runAndAssert("modular", new Int8Datum(10), new Int8Datum(0), NullDatum.get());
    runAndAssert("modular", new Int8Datum(10), new Float4Datum(0.0f), NullDatum.get());
    runAndAssert("modular", new Int8Datum(10), new Float8Datum(0.0), NullDatum.get());
  }

  @Test
  public void testFloat4Datum() throws Exception {
    //plus
    runAndAssert("plus", new Float4Datum(10.0f), new Int2Datum((short)5), new Float4Datum(15.0f));
    runAndAssert("plus", new Float4Datum(10.0f), new Int4Datum(5), new Float4Datum(15.0f));
    runAndAssert("plus", new Float4Datum(10.0f), new Int8Datum(5), new Float4Datum(15.0f));
    runAndAssert("plus", new Float4Datum(10.0f), new Float4Datum(5.0f), new Float4Datum(15.0f));
    runAndAssert("plus", new Float4Datum(10.0f), new Float8Datum(5.0), new Float8Datum(15.0));

    //minus
    runAndAssert("minus", new Float4Datum(10.0f), new Int2Datum((short)5), new Float4Datum(5.0f));
    runAndAssert("minus", new Float4Datum(10.0f), new Int4Datum(5), new Float4Datum(5.0f));
    runAndAssert("minus", new Float4Datum(10.0f), new Int8Datum(5), new Float4Datum(5.0f));
    runAndAssert("minus", new Float4Datum(10.0f), new Float4Datum(5.0f), new Float4Datum(5.0f));
    runAndAssert("minus", new Float4Datum(10.0f), new Float8Datum(5.0), new Float8Datum(5.0));

    runAndAssert("minus", new Float4Datum(5.0f), new Int2Datum((short)10), new Float4Datum(-5.0f));
    runAndAssert("minus", new Float4Datum(5.0f), new Int4Datum(10), new Float4Datum(-5.0f));
    runAndAssert("minus", new Float4Datum(5.0f), new Float4Datum(10.0f), new Float4Datum(-5.0f));
    runAndAssert("minus", new Float4Datum(5.0f), new Float4Datum(10.0f), new Float4Datum(-5.0f));
    runAndAssert("minus", new Float4Datum(5.0f), new Float8Datum(10.0), new Float8Datum(-5.0));

    //multiply
    runAndAssert("multiply", new Float4Datum(10.0f), new Int2Datum((short)5), new Float4Datum(50.0f));
    runAndAssert("multiply", new Float4Datum(10.0f), new Int4Datum(5), new Float4Datum(50.0f));
    runAndAssert("multiply", new Float4Datum(10.0f), new Int8Datum(5), new Float4Datum(50.0f));
    runAndAssert("multiply", new Float4Datum(10.0f), new Float4Datum(5.0f), new Float4Datum(50.0f));
    runAndAssert("multiply", new Float4Datum(10.0f), new Float8Datum(5.0), new Float8Datum(50.0));

    //divide
    runAndAssert("divide", new Float4Datum(10.0f), new Int2Datum((short)5), new Float4Datum(2.0f));
    runAndAssert("divide", new Float4Datum(10.0f), new Int4Datum(5), new Float4Datum(2.0f));
    runAndAssert("divide", new Float4Datum(10.0f), new Int8Datum(5), new Float4Datum(2.0f));
    runAndAssert("divide", new Float4Datum(10.0f), new Float4Datum(5.0f), new Float4Datum(2.0f));
    runAndAssert("divide", new Float4Datum(10.0f), new Float8Datum(5.0), new Float8Datum(2.0));

    runAndAssert("divide", new Float4Datum(10.0f), new Int2Datum((short) 0), NullDatum.get());
    runAndAssert("divide", new Float4Datum(10.0f), new Int4Datum(0), NullDatum.get());
    runAndAssert("divide", new Float4Datum(10.0f), new Int8Datum(0), NullDatum.get());
    runAndAssert("divide", new Float4Datum(10.0f), new Float4Datum(0.0f), NullDatum.get());
    runAndAssert("divide", new Float4Datum(10.0f), new Float8Datum(0.0), NullDatum.get());

    //modular
    runAndAssert("modular", new Float4Datum(10.0f), new Int2Datum((short)3), new Float4Datum(1.0f));
    runAndAssert("modular", new Float4Datum(10.0f), new Int4Datum(3), new Float4Datum(1.0f));
    runAndAssert("modular", new Float4Datum(10.0f), new Int8Datum(3), new Float4Datum(1.0f));
    runAndAssert("modular", new Float4Datum(10.0f), new Float4Datum(3.0f), new Float4Datum(1.0f));
    runAndAssert("modular", new Float4Datum(10.0f), new Float8Datum(3.0), new Float8Datum(1.0));

    runAndAssert("modular", new Float4Datum(10.0f), new Int2Datum((short) 0), NullDatum.get());
    runAndAssert("modular", new Float4Datum(10.0f), new Int4Datum(0), NullDatum.get());
    runAndAssert("modular", new Float4Datum(10.0f), new Int8Datum(0), NullDatum.get());
    runAndAssert("modular", new Float4Datum(10.0f), new Float4Datum(0.0f), NullDatum.get());
    runAndAssert("modular", new Float4Datum(10.0f), new Float8Datum(0.0), NullDatum.get());
  }

  @Test
  public void testFloat8Datum() throws Exception {
    //plus
    runAndAssert("plus", new Float8Datum(10.0), new Int2Datum((short)5), new Float8Datum(15.0));
    runAndAssert("plus", new Float8Datum(10.0), new Int4Datum(5), new Float8Datum(15.0));
    runAndAssert("plus", new Float8Datum(10.0), new Int8Datum(5), new Float8Datum(15.0));
    runAndAssert("plus", new Float8Datum(10.0), new Float4Datum(5.0f), new Float8Datum(15.0));
    runAndAssert("plus", new Float8Datum(10.0), new Float8Datum(5.0), new Float8Datum(15.0));

    //minus
    runAndAssert("minus", new Float8Datum(10.0), new Int2Datum((short)5), new Float8Datum(5.0));
    runAndAssert("minus", new Float8Datum(10.0), new Int4Datum(5), new Float8Datum(5.0));
    runAndAssert("minus", new Float8Datum(10.0), new Int8Datum(5), new Float8Datum(5.0));
    runAndAssert("minus", new Float8Datum(10.0), new Float4Datum(5.0f), new Float8Datum(5.0));
    runAndAssert("minus", new Float8Datum(10.0), new Float8Datum(5.0), new Float8Datum(5.0));

    runAndAssert("minus", new Float8Datum(5.0), new Int2Datum((short)10), new Float8Datum(-5.0));
    runAndAssert("minus", new Float8Datum(5.0), new Int4Datum(10), new Float8Datum(-5.0));
    runAndAssert("minus", new Float8Datum(5.0), new Float8Datum(10.0), new Float8Datum(-5.0));
    runAndAssert("minus", new Float8Datum(5.0), new Float8Datum(10.0), new Float8Datum(-5.0));
    runAndAssert("minus", new Float8Datum(5.0), new Float8Datum(10.0), new Float8Datum(-5.0));

    //multiply
    runAndAssert("multiply", new Float8Datum(10.0), new Int2Datum((short)5), new Float8Datum(50.0));
    runAndAssert("multiply", new Float8Datum(10.0), new Int4Datum(5), new Float8Datum(50.0));
    runAndAssert("multiply", new Float8Datum(10.0), new Int8Datum(5), new Float8Datum(50.0));
    runAndAssert("multiply", new Float8Datum(10.0), new Float4Datum(5.0f), new Float8Datum(50.0));
    runAndAssert("multiply", new Float8Datum(10.0), new Float8Datum(5.0), new Float8Datum(50.0));

    //divide
    runAndAssert("divide", new Float8Datum(10.0), new Int2Datum((short)5), new Float8Datum(2.0));
    runAndAssert("divide", new Float8Datum(10.0), new Int4Datum(5), new Float8Datum(2.0));
    runAndAssert("divide", new Float8Datum(10.0), new Int8Datum(5), new Float8Datum(2.0));
    runAndAssert("divide", new Float8Datum(10.0), new Float4Datum(5.0f), new Float8Datum(2.0));
    runAndAssert("divide", new Float8Datum(10.0), new Float8Datum(5.0), new Float8Datum(2.0));

    runAndAssert("divide", new Float8Datum(10.0), new Int2Datum((short) 0), NullDatum.get());
    runAndAssert("divide", new Float8Datum(10.0), new Int4Datum(0), NullDatum.get());
    runAndAssert("divide", new Float8Datum(10.0), new Int8Datum(0), NullDatum.get());
    runAndAssert("divide", new Float8Datum(10.0), new Float4Datum(0.0f), NullDatum.get());
    runAndAssert("divide", new Float8Datum(10.0), new Float8Datum(0.0), NullDatum.get());

    //modular
    runAndAssert("modular", new Float8Datum(10.0), new Int2Datum((short)3), new Float8Datum(1.0));
    runAndAssert("modular", new Float8Datum(10.0), new Int4Datum(3), new Float8Datum(1.0));
    runAndAssert("modular", new Float8Datum(10.0), new Int8Datum(3), new Float8Datum(1.0));
    runAndAssert("modular", new Float8Datum(10.0), new Float4Datum(3.0f), new Float8Datum(1.0));
    runAndAssert("modular", new Float8Datum(10.0), new Float8Datum(3.0), new Float8Datum(1.0));

    //modular by zero
    runAndAssert("modular", new Float8Datum(10.0), new Int2Datum((short) 0), NullDatum.get());
    runAndAssert("modular", new Float8Datum(10.0), new Int4Datum(0), NullDatum.get());
    runAndAssert("modular", new Float8Datum(10.0), new Int8Datum(0), NullDatum.get());
    runAndAssert("modular", new Float8Datum(10.0), new Float4Datum(0.0f), NullDatum.get());
    runAndAssert("modular", new Float8Datum(10.0), new Float8Datum(0.0), NullDatum.get());
  }

  private void runAndAssert(String op, Datum left, Datum right, Datum expected) throws Exception {
    Datum result = null;
    try {
      if ("plus".equals(op)) {
        result = left.plus(right);
      } else if ("minus".equals(op)) {
        result = left.minus(right);
      } else if ("multiply".equals(op)) {
        result = left.multiply(right);
      } else if ("divide".equals(op)) {
        result = left.divide(right);
        if (right.asFloat8() == 0) {
          if (Datum.abortWhenDivideByZero) {
            fail("should throw DivideByZeroException in the case of nullIfZero false");
          }
        }
      } else if ("modular".equals(op)) {
        result = left.modular(right);
        if (right.asFloat8() == 0) {
          if (Datum.abortWhenDivideByZero) {
            fail("should throw DivideByZeroException in the case of nullIfZero false");
          }
        }
      }
      assertEquals(expected.type(), result.type());
      assertEquals(expected, result);
    } catch (ArithmeticException e) {
      if (!Datum.abortWhenDivideByZero) {
        fail(op + " throws DivideByZeroException");
      } else {
        //success
      }
    }
  }
}
