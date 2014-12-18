/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.tajo.thrift.generated;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2014-12-18")
public class TRowData implements org.apache.thrift.TBase<TRowData, TRowData._Fields>, java.io.Serializable, Cloneable, Comparable<TRowData> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TRowData");

  private static final org.apache.thrift.protocol.TField NULL_FLAGS_FIELD_DESC = new org.apache.thrift.protocol.TField("nullFlags", org.apache.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.thrift.protocol.TField COLUMN_DATAS_FIELD_DESC = new org.apache.thrift.protocol.TField("columnDatas", org.apache.thrift.protocol.TType.LIST, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TRowDataStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TRowDataTupleSchemeFactory());
  }

  public List<Boolean> nullFlags; // required
  public List<ByteBuffer> columnDatas; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    NULL_FLAGS((short)1, "nullFlags"),
    COLUMN_DATAS((short)2, "columnDatas");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // NULL_FLAGS
          return NULL_FLAGS;
        case 2: // COLUMN_DATAS
          return COLUMN_DATAS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.NULL_FLAGS, new org.apache.thrift.meta_data.FieldMetaData("nullFlags", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL))));
    tmpMap.put(_Fields.COLUMN_DATAS, new org.apache.thrift.meta_data.FieldMetaData("columnDatas", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING            , true))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TRowData.class, metaDataMap);
  }

  public TRowData() {
  }

  public TRowData(
    List<Boolean> nullFlags,
    List<ByteBuffer> columnDatas)
  {
    this();
    this.nullFlags = nullFlags;
    this.columnDatas = columnDatas;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TRowData(TRowData other) {
    if (other.isSetNullFlags()) {
      List<Boolean> __this__nullFlags = new ArrayList<Boolean>(other.nullFlags);
      this.nullFlags = __this__nullFlags;
    }
    if (other.isSetColumnDatas()) {
      List<ByteBuffer> __this__columnDatas = new ArrayList<ByteBuffer>(other.columnDatas);
      this.columnDatas = __this__columnDatas;
    }
  }

  public TRowData deepCopy() {
    return new TRowData(this);
  }

  @Override
  public void clear() {
    this.nullFlags = null;
    this.columnDatas = null;
  }

  public int getNullFlagsSize() {
    return (this.nullFlags == null) ? 0 : this.nullFlags.size();
  }

  public java.util.Iterator<Boolean> getNullFlagsIterator() {
    return (this.nullFlags == null) ? null : this.nullFlags.iterator();
  }

  public void addToNullFlags(boolean elem) {
    if (this.nullFlags == null) {
      this.nullFlags = new ArrayList<Boolean>();
    }
    this.nullFlags.add(elem);
  }

  public List<Boolean> getNullFlags() {
    return this.nullFlags;
  }

  public TRowData setNullFlags(List<Boolean> nullFlags) {
    this.nullFlags = nullFlags;
    return this;
  }

  public void unsetNullFlags() {
    this.nullFlags = null;
  }

  /** Returns true if field nullFlags is set (has been assigned a value) and false otherwise */
  public boolean isSetNullFlags() {
    return this.nullFlags != null;
  }

  public void setNullFlagsIsSet(boolean value) {
    if (!value) {
      this.nullFlags = null;
    }
  }

  public int getColumnDatasSize() {
    return (this.columnDatas == null) ? 0 : this.columnDatas.size();
  }

  public java.util.Iterator<ByteBuffer> getColumnDatasIterator() {
    return (this.columnDatas == null) ? null : this.columnDatas.iterator();
  }

  public void addToColumnDatas(ByteBuffer elem) {
    if (this.columnDatas == null) {
      this.columnDatas = new ArrayList<ByteBuffer>();
    }
    this.columnDatas.add(elem);
  }

  public List<ByteBuffer> getColumnDatas() {
    return this.columnDatas;
  }

  public TRowData setColumnDatas(List<ByteBuffer> columnDatas) {
    this.columnDatas = columnDatas;
    return this;
  }

  public void unsetColumnDatas() {
    this.columnDatas = null;
  }

  /** Returns true if field columnDatas is set (has been assigned a value) and false otherwise */
  public boolean isSetColumnDatas() {
    return this.columnDatas != null;
  }

  public void setColumnDatasIsSet(boolean value) {
    if (!value) {
      this.columnDatas = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case NULL_FLAGS:
      if (value == null) {
        unsetNullFlags();
      } else {
        setNullFlags((List<Boolean>)value);
      }
      break;

    case COLUMN_DATAS:
      if (value == null) {
        unsetColumnDatas();
      } else {
        setColumnDatas((List<ByteBuffer>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case NULL_FLAGS:
      return getNullFlags();

    case COLUMN_DATAS:
      return getColumnDatas();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case NULL_FLAGS:
      return isSetNullFlags();
    case COLUMN_DATAS:
      return isSetColumnDatas();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TRowData)
      return this.equals((TRowData)that);
    return false;
  }

  public boolean equals(TRowData that) {
    if (that == null)
      return false;

    boolean this_present_nullFlags = true && this.isSetNullFlags();
    boolean that_present_nullFlags = true && that.isSetNullFlags();
    if (this_present_nullFlags || that_present_nullFlags) {
      if (!(this_present_nullFlags && that_present_nullFlags))
        return false;
      if (!this.nullFlags.equals(that.nullFlags))
        return false;
    }

    boolean this_present_columnDatas = true && this.isSetColumnDatas();
    boolean that_present_columnDatas = true && that.isSetColumnDatas();
    if (this_present_columnDatas || that_present_columnDatas) {
      if (!(this_present_columnDatas && that_present_columnDatas))
        return false;
      if (!this.columnDatas.equals(that.columnDatas))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_nullFlags = true && (isSetNullFlags());
    list.add(present_nullFlags);
    if (present_nullFlags)
      list.add(nullFlags);

    boolean present_columnDatas = true && (isSetColumnDatas());
    list.add(present_columnDatas);
    if (present_columnDatas)
      list.add(columnDatas);

    return list.hashCode();
  }

  @Override
  public int compareTo(TRowData other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetNullFlags()).compareTo(other.isSetNullFlags());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNullFlags()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.nullFlags, other.nullFlags);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetColumnDatas()).compareTo(other.isSetColumnDatas());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetColumnDatas()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.columnDatas, other.columnDatas);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TRowData(");
    boolean first = true;

    sb.append("nullFlags:");
    if (this.nullFlags == null) {
      sb.append("null");
    } else {
      sb.append(this.nullFlags);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("columnDatas:");
    if (this.columnDatas == null) {
      sb.append("null");
    } else {
      sb.append(this.columnDatas);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TRowDataStandardSchemeFactory implements SchemeFactory {
    public TRowDataStandardScheme getScheme() {
      return new TRowDataStandardScheme();
    }
  }

  private static class TRowDataStandardScheme extends StandardScheme<TRowData> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TRowData struct) throws TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // NULL_FLAGS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list18 = iprot.readListBegin();
                struct.nullFlags = new ArrayList<Boolean>(_list18.size);
                boolean _elem19;
                for (int _i20 = 0; _i20 < _list18.size; ++_i20)
                {
                  _elem19 = iprot.readBool();
                  struct.nullFlags.add(_elem19);
                }
                iprot.readListEnd();
              }
              struct.setNullFlagsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // COLUMN_DATAS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list21 = iprot.readListBegin();
                struct.columnDatas = new ArrayList<ByteBuffer>(_list21.size);
                ByteBuffer _elem22;
                for (int _i23 = 0; _i23 < _list21.size; ++_i23)
                {
                  _elem22 = iprot.readBinary();
                  struct.columnDatas.add(_elem22);
                }
                iprot.readListEnd();
              }
              struct.setColumnDatasIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TRowData struct) throws TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.nullFlags != null) {
        oprot.writeFieldBegin(NULL_FLAGS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.BOOL, struct.nullFlags.size()));
          for (boolean _iter24 : struct.nullFlags)
          {
            oprot.writeBool(_iter24);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.columnDatas != null) {
        oprot.writeFieldBegin(COLUMN_DATAS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.columnDatas.size()));
          for (ByteBuffer _iter25 : struct.columnDatas)
          {
            oprot.writeBinary(_iter25);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TRowDataTupleSchemeFactory implements SchemeFactory {
    public TRowDataTupleScheme getScheme() {
      return new TRowDataTupleScheme();
    }
  }

  private static class TRowDataTupleScheme extends TupleScheme<TRowData> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TRowData struct) throws TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetNullFlags()) {
        optionals.set(0);
      }
      if (struct.isSetColumnDatas()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetNullFlags()) {
        {
          oprot.writeI32(struct.nullFlags.size());
          for (boolean _iter26 : struct.nullFlags)
          {
            oprot.writeBool(_iter26);
          }
        }
      }
      if (struct.isSetColumnDatas()) {
        {
          oprot.writeI32(struct.columnDatas.size());
          for (ByteBuffer _iter27 : struct.columnDatas)
          {
            oprot.writeBinary(_iter27);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TRowData struct) throws TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list28 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.BOOL, iprot.readI32());
          struct.nullFlags = new ArrayList<Boolean>(_list28.size);
          boolean _elem29;
          for (int _i30 = 0; _i30 < _list28.size; ++_i30)
          {
            _elem29 = iprot.readBool();
            struct.nullFlags.add(_elem29);
          }
        }
        struct.setNullFlagsIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list31 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.columnDatas = new ArrayList<ByteBuffer>(_list31.size);
          ByteBuffer _elem32;
          for (int _i33 = 0; _i33 < _list31.size; ++_i33)
          {
            _elem32 = iprot.readBinary();
            struct.columnDatas.add(_elem32);
          }
        }
        struct.setColumnDatasIsSet(true);
      }
    }
  }

}

