package org.apache.tajo.storage.orc.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tajo.datum.Datum;

public class TajoBlobObjectInspector extends TajoPrimitiveObjectInspector implements BinaryObjectInspector {
  @Override
  public PrimitiveTypeInfo getTypeInfo() {
    return TypeInfoFactory.binaryTypeInfo;
  }

  @Override
  public PrimitiveCategory getPrimitiveCategory() {
    return PrimitiveCategory.BINARY;
  }

  @Override
  public Class<?> getPrimitiveWritableClass() {
    return null;
  }

  @Override
  public BytesWritable getPrimitiveWritableObject(Object o) {
    return null;
  }

  @Override
  public Class<?> getJavaPrimitiveClass() {
    return byte [].class;
  }

  @Override
  public byte[] getPrimitiveJavaObject(Object o) {
    return ((Datum)o).asByteArray();
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
  public int precision() {
    return 0;
  }

  @Override
  public int scale() {
    return 0;
  }

  @Override
  public String getTypeName() {
    return "BINARY";
  }
}
