package nta.engine.ipc.protocolrecords;

/**
 * AssignTabletRequest contains a set of Tablet, each of which is a partitioned table.
 * 
 * @author hyunsik 
 *
 */
public interface AssignTabletRequest {
	Fragment [] getTablets();
}
