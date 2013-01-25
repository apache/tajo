/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.datum;

public class DatumFactory {
  public static NullDatum createNullDatum() {
    return NullDatum.get();
  }
  
  public static BoolDatum createBool(String val) {
    boolean boolVal = val.equalsIgnoreCase("true");
    return new BoolDatum(boolVal);
  }
  
  public static BoolDatum createBool(byte val) {
    boolean boolVal = val == 0x01;
    return new BoolDatum(boolVal);
  }
  
  public static BoolDatum createBool(boolean val) {
    return new BoolDatum(val);
  }
  /*
  public static BoolDatum createBool(int val) {
    return new BoolDatum(val);
  }
  */
  
	public static ByteDatum createByte(byte val) {
		return new ByteDatum(val);
	}
  /*
  public static ByteDatum createByte(int val) {
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
	
	public static ShortDatum createShort(short val) {
		return new ShortDatum(val);
	}
	
	public static ShortDatum createShort(String val) {
	  return new ShortDatum(Short.valueOf(val));
	}
	
	public static IntDatum createInt(int val) {
		return new IntDatum(val);
	}
	
	public static IntDatum createInt(String val) {
	  return new IntDatum(Integer.valueOf(val));
	}
	
	public static LongDatum createLong(long val) {
		return new LongDatum(val);
	}
	
	public static LongDatum createLong(String val) {
	  return new LongDatum(Long.valueOf(val));
	}
	
	public static FloatDatum createFloat(float val) {
		return new FloatDatum(val);
	}
	
	public static FloatDatum createFloat(String val) {
	  return new FloatDatum(Float.valueOf(val));
	}
	
	public static DoubleDatum createDouble(double val) {
		return new DoubleDatum(val);
	}
	
	public static DoubleDatum createDouble(String val) {
	  return new DoubleDatum(Double.valueOf(val));
	}
	
  public static StringDatum createString(String val) {
    return new StringDatum(val);
  }

  public static StringDatum2 createString2(String val) {
    return new StringDatum2(val);
  }

  public static StringDatum2 createString2(byte[] val) {
    return new StringDatum2(val);
  }

  public static StringDatum createString(byte[] val) {
    return new StringDatum(val);
  }
	
	public static BytesDatum createBytes(byte [] val) {
    return new BytesDatum(val);
  }
	
	public static BytesDatum createBytes(String val) {
	  return new BytesDatum(val.getBytes());
	}
	
	public static IPv4Datum createIPv4(byte [] val) {
	  return new IPv4Datum(val);
	}
	
	public static IPv4Datum createIPv4(String val) {
	  return new IPv4Datum(val);
	}
}