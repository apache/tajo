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
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.AnyDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;
import org.python.core.*;

public class JythonUtils {

//  /**
//   * @param schemaString a String representation of the Schema <b>without</b>
//   *                     any enclosing curly-braces.<b>Not</b> for use with
//   *                     <code>Schema#toString</code>
//   * @return Schema instance
//   * @throws ParserException
//   */
//  public static Schema getSchemaFromString(String schemaString) {
//    LogicalSchema schema = parseSchema(schemaString);
//    Schema result = org.apache.pig.newplan.logical.Util.translateSchema(schema);
//    Schema.setSchemaDefaultType(result, DataType.BYTEARRAY);
//    return result;
//  }
//
//  public static LogicalSchema parseSchema(String schemaString) {
//    QueryParserDriver queryParser = new QueryParserDriver( new PigContext(),
//        "util", new HashMap<String, String>() ) ;
//    LogicalSchema schema = queryParser.parseSchema(schemaString);
//    return schema;
//  }

  public static PyObject datumToPyObject(Datum v) {
    Preconditions.checkArgument(v.type() == TajoDataTypes.Type.ANY);
    Datum actual = ((AnyDatum) v).getActual();
    switch (actual.type()) {
      case NULL_TYPE:
        return Py.java2py(null);
      case BOOLEAN:
        return Py.java2py(actual.asBool());
      case UINT1:
      case INT1:
        return Py.java2py(actual.asInt2());
      case UINT2:
      case INT2:
        return Py.java2py(actual.asInt2());
      case UINT4:
      case INT4:
        return Py.java2py(actual.asInt4());
      case UINT8:
      case INT8:
        return Py.java2py(actual.asInt8());
      case FLOAT4:
      case FLOAT8:
        return Py.java2py(actual.asFloat8());
      case CHAR:
      case VARCHAR:
      case TEXT:
        return Py.java2py(actual.asChars());
      case NCHAR:
      case NVARCHAR:
        return Py.java2py(actual.asUnicodeChars());
      case BLOB:
        return Py.java2py(actual.asByteArray());
      case INET4:
        return Py.java2py(actual.asByteArray());
      case INET6:
        return Py.java2py(actual.asByteArray());
      default:
        throw new UnsupportedException("Unsupported type: " + actual.type());
    }

//    if (object instanceof Tuple) {
//      return tupleToPyTuple((Tuple) object);
//    } else if (object instanceof DataBag) {
//      PyList list = new PyList();
//      for (Tuple bagTuple : (DataBag) object) {
//        list.add(tupleToPyTuple(bagTuple));
//      }
//      return list;
//    } else if (object instanceof Map<?, ?>) {
//      PyDictionary newMap = new PyDictionary();
//      for (Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
//        newMap.put(entry.getKey(), datumToPyObject(entry.getValue()));
//      }
//      return newMap;
//    } else if (object instanceof DataByteArray) {
//      return Py.java2py(((DataByteArray) object).get());
//    } else {
//      return Py.java2py(object);
//    }
  }

  public static PyTuple tupleToPyTuple(Tuple tuple) {
    PyObject[] pyTuple = new PyObject[tuple.size()];
    int i = 0;
    for (Datum v : tuple.getValues()) {
      pyTuple[i++] = datumToPyObject(v);
    }
    return new PyTuple(pyTuple);
  }

  public static Datum pyObjectToDatum(PyObject object) {
    if (object instanceof PyLong) {
      return DatumFactory.createInt8((Long) object.__tojava__(Long.class));
    } else if (object instanceof PyBoolean) {
      return DatumFactory.createBool((Boolean) object.__tojava__(Boolean.class));
    } else if (object instanceof PyInteger) {
      return DatumFactory.createInt4((Integer) object.__tojava__(Integer.class));
    } else if (object instanceof PyFloat) {
      // J(P)ython is loosely typed, supports only float type,
      // hence we convert everything to double to save precision
      return DatumFactory.createFloat8((Double) object.__tojava__(Double.class));
    } else if (object instanceof PyString) {
      return DatumFactory.createText((String) object.__tojava__(String.class));
    } else if (object instanceof PyNone) {
      return DatumFactory.createNullDatum();
    } else if (object instanceof PyTuple) {
      throw new UnsupportedException("Not supported data type: " + object.getClass().getName());
    } else if (object instanceof PyList) {
      throw new UnsupportedException("Not supported data type: " + object.getClass().getName());
    } else if (object instanceof PyDictionary) {
      throw new UnsupportedException("Not supported data type: " + object.getClass().getName());
    } else {
      Object javaObj = object.__tojava__(byte[].class);
      if(javaObj instanceof byte[]) {
        return DatumFactory.createBlob((byte[]) javaObj);
      }
      else {
        throw new UnsupportedException("Not supported data type: " + object.getClass().getName());
      }
    }
  }
}
