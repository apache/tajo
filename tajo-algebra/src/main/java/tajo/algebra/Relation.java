package tajo.algebra;

import tajo.util.TUtil;

public class Relation extends Expr {
  private String rel_name;
  private String alias;

  protected Relation(ExprType type, String relationName) {
    super(type);
    this.rel_name = relationName;
  }

  public Relation(String relationName) {
    this(ExprType.Relation, relationName);
  }

  public String getName() {
    return rel_name;
  }

  public boolean hasAlias() {
    return alias != null;
  }

  public String getAlias() {
    return this.alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }

  @Override
  boolean equalsTo(Expr expr) {
    Relation other = (Relation) expr;
    return TUtil.checkEquals(rel_name, other.rel_name) &&
        TUtil.checkEquals(alias, other.alias);
  }

  @Override
  public int hashCode() {
    int result = rel_name.hashCode();
    result = 31 * result + (alias != null ? alias.hashCode() : 0);
    return result;
  }
}
