package nta.storage;

import java.nio.ByteBuffer;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.datum.DatumFactory;

public class TupleUtil {
  public static byte [] toBytes(Schema schema, Tuple tuple) {
    int size = StorageUtil.getRowByteSize(schema);
    ByteBuffer bb = ByteBuffer.allocate(size);
    Column col;
    for (int i = 0; i < schema.getColumnNum(); i++) {
      col = schema.getColumn(i);
      switch (col.getDataType()) {
      case BYTE: bb.put(tuple.get(i).asByte()); break;
      case CHAR: bb.put(tuple.get(i).asByte()); break;
      case BOOLEAN: bb.put(tuple.get(i).asByte()); break;
      case SHORT: bb.putShort(tuple.get(i).asShort()); break;
      case INT: bb.putInt(tuple.get(i).asInt()); break;
      case LONG: bb.putLong(tuple.get(i).asLong()); break;
      case FLOAT: bb.putFloat(tuple.get(i).asFloat()); break;
      case DOUBLE: bb.putDouble(tuple.get(i).asDouble()); break;
      case STRING: 
        byte [] _string = tuple.get(i).asByteArray();
        bb.putInt(_string.length);
        bb.put(_string);
        break;
      case BYTES: 
        byte [] bytes = tuple.get(i).asByteArray();
        bb.putInt(bytes.length);
        bb.put(bytes); 
        break;
      case IPv4:
        byte [] ipBytes = tuple.getIPv4Bytes(i);
        bb.put(ipBytes);
        break;
      case IPv6: bb.put(tuple.getIPv6Bytes(i)); break;
      default: 
      }
    }
    
    bb.flip();
    byte [] buf = new byte [bb.limit()];
    bb.get(buf);
    return buf;
  }
  
  public static Tuple toTuple(Schema schema, byte [] bytes) {
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    Tuple tuple = new VTuple(schema.getColumnNum());
    Column col;
    for (int i =0; i < schema.getColumnNum(); i++) {
      col = schema.getColumn(i);
      
      switch (col.getDataType()) {
      case BYTE: tuple.put(i, DatumFactory.createByte(bb.get())); break;
      case CHAR: tuple.put(i, DatumFactory.createChar(bb.get())); break;
      case BOOLEAN: tuple.put(i, DatumFactory.createBool(bb.get())); break;
      case SHORT: tuple.put(i, DatumFactory.createShort(bb.getShort())); break;
      case INT: tuple.put(i, DatumFactory.createInt(bb.getInt())); break;
      case LONG: tuple.put(i, DatumFactory.createLong(bb.getLong())); break;
      case FLOAT: tuple.put(i, DatumFactory.createFloat(bb.getFloat())); break;
      case DOUBLE: tuple.put(i, DatumFactory.createDouble(bb.getDouble())); break;
      case STRING:
        byte [] _string = new byte[bb.getInt()];
        bb.get(_string);
        tuple.put(i, DatumFactory.createString(new String(_string)));
        break;
      case BYTES: 
        byte [] _bytes = new byte[bb.getInt()];
        bb.get(_bytes);
        tuple.put(i, DatumFactory.createBytes(_bytes)); 
        break;
      case IPv4:
        byte [] _ipv4 = new byte[4];
        bb.get(_ipv4);
        tuple.put(i, DatumFactory.createIPv4(_ipv4));
        break;
      case IPv6:
        // TODO - to be implemented
      }
    }
    return tuple;
  }
}
