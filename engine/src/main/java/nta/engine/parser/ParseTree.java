package nta.engine.parser;

public abstract class ParseTree {
  protected final StatementType type;
  
  public ParseTree(final StatementType type) {
    this.type = type;
  }
  
  public StatementType getType() {
    return this.type;
  }
}
