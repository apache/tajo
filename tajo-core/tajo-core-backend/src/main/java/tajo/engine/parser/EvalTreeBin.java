package tajo.engine.parser;

/**
 * 
 * Serialized된 evaluation tree를 담는다. 
 * 
 * @author Hyunsik Choi
 *
 */
public class EvalTreeBin {
  private byte [] bytecode;
  
  public EvalTreeBin(final byte [] bytecode) {
    this.bytecode = bytecode;
  }
  
  public final byte [] getBinary() {
    return bytecode;
  }
}
