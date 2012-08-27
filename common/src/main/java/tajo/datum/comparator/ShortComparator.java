package tajo.datum.comparator;

import tajo.datum.ShortDatum;

import java.util.Comparator;

public class ShortComparator implements Comparator<ShortDatum> {

	@Override
	public int compare(ShortDatum o1, ShortDatum o2) {
		return o1.compareTo(o2);
	}

}
