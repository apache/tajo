package nta.cube;

import nta.datum.Datum;

/* SummaryTable의 tuple이 될 structure */
public class KVpair implements Comparable<KVpair> {
	Datum[] key;
	Datum[] val;
	int count;

	public KVpair() {
		key = new Datum[Cons.groupnum];
		val = new Datum[Cons.measurenum];
	}
	
	
	@Override
	public int compareTo(KVpair o) {
		for (int k = 0; k < Cons.groupnum; k++) {
			try {
				if (this.key[k].lessThan(o.key[k]).asBool()) {
					return -1;
				} else if (this.key[k].greaterThan(o.key[k]).asBool()) {
					return 1;
				} else {
					continue;
				}
			} catch (Exception e) {
				continue;
			}
		}
		return 0;
	}

	public String toString() {
		StringBuilder toReturn = new StringBuilder();
		for (int k = 0; k < Cons.groupnum; k++) {
			toReturn.append(key[k].toString() + " ");
		}
		for (int k = 0; k < Cons.measurenum; k++) {
			toReturn.append(val[k].toString() + " ");
		}
		toReturn.append(count);
		return toReturn.toString();
	}
}
