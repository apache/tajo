package nta.engine.utils;

import nta.rpc.protocolrecords.PrimitiveProtos.StringProto;

public class ProtoUtil {
  public static StringProto newProto(String val) {
    StringProto.Builder builder = StringProto.newBuilder();
    builder.setValue(val);
    return builder.build();
  }
}
