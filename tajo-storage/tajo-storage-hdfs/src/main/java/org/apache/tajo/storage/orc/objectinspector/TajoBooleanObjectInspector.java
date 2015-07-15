package org.apache.tajo.storage.orc.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.tajo.datum.Datum;

public class TajoBooleanObjectInspector extends TajoPrimitiveObjectInspector implements BooleanObjectInspector {
  @Override
  public boolean get(Object o) {
    return ((Datum)o).asBool();
  }

  @Override
  public PrimitiveTypeInfo getTypeInfo() {
    return TypeInfoFactory.booleanTypeInfo;
  }

  @Override
  public PrimitiveCategory getPrimitiveCategory() {
    return PrimitiveCategory.BOOLEAN;
  }

  @Override
  public Class<?> getPrimitiveWritableClass() {
    return null;
  }

  @Override
  public Object getPrimitiveWritableObject(Object o) {
    return null;
  }

  @Override
  public Class<?> getJavaPrimitiveClass() {
    return Boolean.class;
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    return null;
  }

  @Override
  public Object copyObject(Object o) {
    return null;
  }

  @Override
  public boolean preferWritable() {
    return false;
  }

  @Override
  public String getTypeName() {
    return "BOOLEAN";
  }
}
