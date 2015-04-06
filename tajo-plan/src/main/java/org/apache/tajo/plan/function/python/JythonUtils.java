/*
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

package org.apache.tajo.plan.function.python;

import com.google.common.base.Preconditions;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.AnyDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;
//import org.python.core.*;

public class JythonUtils {

//  /**
//   * Convert a datum to a PyObject.
//   * @param v
//   * @return
//   */
//  public static PyObject datumToPyObject(Datum v) {
//    Preconditions.checkArgument(v.type() == TajoDataTypes.Type.ANY);
//    Datum actual = ((AnyDatum) v).getActual();
//    switch (actual.type()) {
//      case NULL_TYPE:
//        return Py.java2py(null);
//      case BOOLEAN:
//        return Py.java2py(actual.asBool());
//      case UINT1:
//      case INT1:
//        return Py.java2py(actual.asInt2());
//      case UINT2:
//      case INT2:
//        return Py.java2py(actual.asInt2());
//      case UINT4:
//      case INT4:
//        return Py.java2py(actual.asInt4());
//      case UINT8:
//      case INT8:
//        return Py.java2py(actual.asInt8());
//      case FLOAT4:
//      case FLOAT8:
//        return Py.java2py(actual.asFloat8());
//      case CHAR:
//      case VARCHAR:
//      case TEXT:
//        return Py.java2py(actual.asChars());
//      case NCHAR:
//      case NVARCHAR:
//        return Py.java2py(actual.asUnicodeChars());
//      case BLOB:
//        return Py.java2py(actual.asByteArray());
//      case INET4:
//        return Py.java2py(actual.asByteArray());
//      case INET6:
//        return Py.java2py(actual.asByteArray());
//      default:
//        throw new UnsupportedException("Unsupported type: " + actual.type());
//    }
//  }
//
//  /**
//   * Convert a Tajo tuple to a PyTuple
//   * @param tuple
//   * @return
//   */
//  public static PyTuple tupleToPyTuple(Tuple tuple) {
//    PyObject[] pyTuple = new PyObject[tuple.size()];
//    int i = 0;
//    for (Datum v : tuple.getValues()) {
//      pyTuple[i++] = datumToPyObject(v);
//    }
//    return new PyTuple(pyTuple);
//  }

  public static Datum objectToDatum(TajoDataTypes.DataType type, Object o) {
    switch (type.getType()) {
      case BOOLEAN:
        return DatumFactory.createBool((Boolean) o);
      case INT1:
      case INT2:
        return DatumFactory.createInt2((Short) o);
      case INT4:
        return DatumFactory.createInt4((Integer) o);
      case INT8:
        return DatumFactory.createInt8((Long) o);
      case UINT1:
      case UINT2:
        return DatumFactory.createInt2((Short) o);
      case UINT4:
        return DatumFactory.createInt4((Integer) o);
      case UINT8:
        return DatumFactory.createInt8((Long) o);
      case FLOAT4:
        return DatumFactory.createFloat4((Float) o);
      case FLOAT8:
        return DatumFactory.createFloat8((Double) o);
      case CHAR:
        return DatumFactory.createChar((Character) o);
      case TEXT:
        return DatumFactory.createText((String) o);
      case DATE:
        return DatumFactory.createDate((Integer) o);
      case TIME:
        return DatumFactory.createTime((Long) o);
      case TIMESTAMP:
        return DatumFactory.createTimestamp((Long) o);
      case INTERVAL:
        return DatumFactory.createInterval((Long) o);
      case BLOB:
        return DatumFactory.createBlob((byte[]) o);
      case INET4:
        return DatumFactory.createInet4((Integer) o);
      default:
        throw new UnsupportedException(type.toString());
    }
  }

//  /**
//   * Convert a PyObject to a datum.
//   * @param object
//   * @return
//   */
//  public static Datum pyObjectToDatum(PyObject object) {
//    if (object instanceof PyLong) {
//      return DatumFactory.createInt8((Long) object.__tojava__(Long.class));
//    } else if (object instanceof PyBoolean) {
//      return DatumFactory.createBool((Boolean) object.__tojava__(Boolean.class));
//    } else if (object instanceof PyInteger) {
//      return DatumFactory.createInt4((Integer) object.__tojava__(Integer.class));
//    } else if (object instanceof PyFloat) {
//      // J(P)ython is loosely typed, supports only float type,
//      // hence we convert everything to double to save precision
//      return DatumFactory.createFloat8((Double) object.__tojava__(Double.class));
//    } else if (object instanceof PyString) {
//      return DatumFactory.createText((String) object.__tojava__(String.class));
//    } else if (object instanceof PyNone) {
//      return DatumFactory.createNullDatum();
//    } else if (object instanceof PyTuple) {
//      throw new UnsupportedException("Not supported data type: " + object.getClass().getName());
//    } else if (object instanceof PyList) {
//      throw new UnsupportedException("Not supported data type: " + object.getClass().getName());
//    } else if (object instanceof PyDictionary) {
//      throw new UnsupportedException("Not supported data type: " + object.getClass().getName());
//    } else {
//      Object javaObj = object.__tojava__(byte[].class);
//      if(javaObj instanceof byte[]) {
//        return DatumFactory.createBlob((byte[]) javaObj);
//      }
//      else {
//        throw new UnsupportedException("Not supported data type: " + object.getClass().getName());
//      }
//    }
//  }
//
//  /**
//   * Convert a pyObject to a datum of the given type.
//   * @param object an object will be converted to a datum.
//   * @param type target datum type.
//   * @return a datum of the given type.
//   */
//  public static Datum pyObjectToDatum(PyObject object, TajoDataTypes.Type type) {
//    return DatumFactory.cast(pyObjectToDatum(object), CatalogUtil.newSimpleDataType(type), null);
//  }

  /**
   * Convert the primitive type to the Tajo type.
   * @param clazz
   * @return
   */
  public static TajoDataTypes.Type primitiveTypeToDataType(Class clazz) {
    if (clazz.getName().equals(Long.class.getName())) {
      return TajoDataTypes.Type.INT8;
    } else if (clazz.getName().equals(Boolean.class.getName())) {
      return TajoDataTypes.Type.BOOLEAN;
    } else if (clazz.getName().equals(Integer.class.getName())) {
      return TajoDataTypes.Type.INT4;
    } else if (clazz.getName().equals(Float.class.getName())) {
      // J(P)ython is loosely typed, supports only float type,
      // hence we convert everything to double to save precision
      return TajoDataTypes.Type.FLOAT4;
    } else if (clazz.getName().equals(Double.class.getName())) {
      return TajoDataTypes.Type.FLOAT8;
    } else if (clazz.getName().equals(String.class.getName())) {
      return TajoDataTypes.Type.TEXT;
    } else {
      if(clazz.getName().equals(byte[].class.getName())) {
        return TajoDataTypes.Type.BLOB;
      }
      else {
        throw new UnsupportedException("Not supported data type: " + clazz.getName());
      }
    }
  }

  /**
   * Convert the Tajo type to the primitive type.
   * @param type
   * @return
   */
  public static Object dataTypeToPrimitiveType(TajoDataTypes.Type type) {
    switch (type) {
      case BOOLEAN:
        return Boolean.class;
      case UINT1:
      case INT1:
      case UINT2:
      case INT2:
        return Short.class;
      case UINT4:
      case INT4:
        return Integer.class;
      case UINT8:
      case INT8:
        return Long.class;
      case FLOAT4:
        return Float.class;
      case FLOAT8:
        return Double.class;
      case CHAR:
      case VARCHAR:
        return Character.class;
      case TEXT:
      case NCHAR:
      case NVARCHAR:
        return String.class;
      case BLOB:
        return Byte[].class;
      case INET4:
      case INET6:
        return Byte[].class;
      default:
        throw new UnsupportedException("Unsupported type: " + type);
    }
  }
}
