package tajo.datum.comparator;

import tajo.datum.IPv6Datum;

import java.util.Comparator;

public class IPv6Comparator implements Comparator<IPv6Datum> {

	@Override
	public int compare(IPv6Datum o1, IPv6Datum o2) {
		return o1.compareTo(o2);
	}

}
