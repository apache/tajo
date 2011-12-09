package nta.datum.comparator;
import java.util.Comparator;

import nta.datum.LongDatum;


public class LongComparator implements Comparator<LongDatum> {

	@Override
	public int compare(LongDatum o1, LongDatum o2) {
		return o1.compareTo(o2);
	}

}
