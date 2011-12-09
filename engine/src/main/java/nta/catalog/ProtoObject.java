package nta.catalog;

import com.google.protobuf.Message;

public interface ProtoObject<P extends Message> {
	public P getProto();
}
