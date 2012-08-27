package tajo.datum.comparator;

import tajo.datum.ByteDatum;

import java.util.Comparator;

public class ByteComparator implements Comparator<ByteDatum> {

	@Override
	public int compare(ByteDatum o1, ByteDatum o2) {
		return o1.compareTo(o2);
	}

}
