package nta.cube;

/* 숫자계산 function class */
public class SomeFunctions {
  public static int power(int a, int b) {
    int toReturn = 1;
    for (int k = 0; k < b; k++) {
      toReturn *= a;
    }
    return toReturn;
  }

  // public static int cuboidparent(CubeConf conf, int cuboid_number, int
  // total_cuboid) {
  // int toReturn = cuboid_number;
  // int cuboidnum = cuboid_number;
  // for (int k = 0; k < conf.getOutschema().getColumnNum(); k++) {
  // if ((cuboidnum) % 2 == 0) {
  // return toReturn + power(2, k);
  // }
  // cuboidnum = cuboidnum >>> 1;
  // }
  // return power(2, conf.getOutschema().getColumnNum());
  // }

  // public static int countone(int f) {
  // int count = 0;
  // while(f > 0){
  // if (f % 2 == 1) {
  // count++;
  // }
  // f = f >>> 1;
  // }
  // return count;
  // }
  // public static int countone(FieldChecker f) {
  // int count = 0;
  // for (int k = 0; k < f.fieldchecker.length; k++) {
  // if(f.check(k)){
  // count++;
  // }
  // }
  // return count;
  // }

  public static int fieldgen(int[] i) {
    int toReturn = 0;
    for (int k = 0; k < i.length; k++) {
      toReturn += power(2, i[k]);
    }
    return toReturn;
  }
}
