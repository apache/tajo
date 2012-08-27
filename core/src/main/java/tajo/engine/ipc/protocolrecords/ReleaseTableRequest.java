package tajo.engine.ipc.protocolrecords;

public interface ReleaseTableRequest {
	Fragment [] getTablets();
}
