package nta.engine.ipc;

import nta.engine.Abortable;
import nta.engine.Stoppable;
import nta.engine.ipc.protocolrecords.ExecutorRunRequest;
import nta.engine.ipc.protocolrecords.SubQueryResponse;

public interface ExecutorRunnerInterface extends Stoppable, Abortable {

	public SubQueryResponse requestExecutorRunning(ExecutorRunRequest request);
	
}
