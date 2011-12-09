package nta.datum.comparator;

import java.util.Comparator;

import nta.datum.ByteDatum;

public class ByteComparator implements Comparator<ByteDatum> {

	@Override
	public int compare(ByteDatum o1, ByteDatum o2) {
		return o1.compareTo(o2);
	}

}
