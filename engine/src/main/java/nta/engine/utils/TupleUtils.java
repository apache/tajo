package nta.engine.utils;

import nta.datum.Datum;
import nta.engine.query.TargetEntry;
import nta.storage.VTuple;

public class TupleUtils {
	public static VTuple project(VTuple intoTuple, TargetEntry [] targets, Datum [] datum) {		
		for(int i=0; i < targets.length; i++) {
			intoTuple.put(targets[i].resId, datum[i]);
		}
		
		return intoTuple;
	}
}
