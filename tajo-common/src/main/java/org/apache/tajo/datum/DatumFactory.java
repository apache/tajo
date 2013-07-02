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

public class DatumFactory {
  public static NullDatum createNullDatum() {
    return NullDatum.get();
  }
  
  public static BooleanDatum createBool(String val) {
    boolean boolVal = val.equalsIgnoreCase("true");
    return new BooleanDatum(boolVal);
  }
  
  public static BooleanDatum createBool(byte val) {
    boolean boolVal = val == 0x01;
    return new BooleanDatum(boolVal);
  }
  
  public static BooleanDatum createBool(boolean val) {
    return new BooleanDatum(val);
  }
  /*
  public static BoolDatum createBool(int val) {
    return new BoolDatum(val);
  }
  */
  
	public static BitDatum createBit(byte val) {
		return new BitDatum(val);
	}
  /*
  public static ByteDatum createBit(int val) {
    return new ByteDatum(val);
  }*/

  public static CharDatum createChar(char val) {
    return new CharDatum(val);
  }

  public static CharDatum createChar(byte val) {
    return new CharDatum(val);
  }

  /*
  public static CharDatum createChar(Integer val) {
    return new CharDatum(val);
  }
  */
	
	public static Int2Datum createInt2(short val) {
		return new Int2Datum(val);
	}
	
	public static Int2Datum createInt2(String val) {
	  return new Int2Datum(Short.valueOf(val));
	}
	
	public static Int4Datum createInt4(int val) {
		return new Int4Datum(val);
	}
	
	public static Int4Datum createInt4(String val) {
	  return new Int4Datum(Integer.valueOf(val));
	}
	
	public static Int8Datum createInt8(long val) {
		return new Int8Datum(val);
	}
	
	public static Int8Datum createInt8(String val) {
	  return new Int8Datum(Long.valueOf(val));
	}
	
	public static Float4Datum createFloat4(float val) {
		return new Float4Datum(val);
	}
	
	public static Float4Datum createFloat4(String val) {
	  return new Float4Datum(Float.valueOf(val));
	}
	
	public static Float8Datum createFloat8(double val) {
		return new Float8Datum(val);
	}
	
	public static Float8Datum createFloat8(String val) {
	  return new Float8Datum(Double.valueOf(val));
	}
	
  public static TextDatum createText(String val) {
    return new TextDatum(val);
  }

  public static TextDatum createText(byte[] val) {
    return new TextDatum(val);
  }
	
	public static BlobDatum createBlob(byte[] val) {
    return new BlobDatum(val);
  }
	
	public static BlobDatum createBlob(String val) {
	  return new BlobDatum(val.getBytes());
	}
	
	public static Inet4Datum createInet4(byte[] val) {
	  return new Inet4Datum(val);
	}
	
	public static Inet4Datum createInet4(String val) {
	  return new Inet4Datum(val);
	}
}