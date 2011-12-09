package nta.rpc;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

public interface LogRPCProtocol extends VersionedProtocol {

	public long versionID = 0;
	public BytesWritable getLogs() throws IOException;
}
