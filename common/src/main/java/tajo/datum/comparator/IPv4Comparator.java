package tajo.datum.comparator;

import tajo.datum.IPv4Datum;

import java.util.Comparator;

public class IPv4Comparator implements Comparator<IPv4Datum> {

	@Override
	public int compare(IPv4Datum o1, IPv4Datum o2) {
		return o1.compareTo(o2);
	}

}
