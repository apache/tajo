package nta.datum.comparator;

import java.util.Comparator;

import nta.datum.ShortDatum;

public class ShortComparator implements Comparator<ShortDatum> {

	@Override
	public int compare(ShortDatum o1, ShortDatum o2) {
		return o1.compareTo(o2);
	}

}
