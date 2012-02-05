package nta.common;

import com.google.protobuf.Message;

public interface ProtoObject<P extends Message> {
	public void initFromProto();
	public P getProto();
}
