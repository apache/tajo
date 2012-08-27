package tajo.datum.comparator;

import tajo.datum.EnumDatum;

import java.util.Comparator;

public class EnumComparator implements Comparator<EnumDatum> {

	@Override
	public int compare(EnumDatum arg0, EnumDatum arg1) {
		return arg0.compareTo(arg1);
	}

}
