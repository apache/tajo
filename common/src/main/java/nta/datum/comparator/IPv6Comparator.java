package nta.datum.comparator;

import java.util.Comparator;

import nta.datum.IPv6Datum;

public class IPv6Comparator implements Comparator<IPv6Datum> {

	@Override
	public int compare(IPv6Datum o1, IPv6Datum o2) {
		return o1.compareTo(o2);
	}

}
