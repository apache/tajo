package org.apache.tajo.rule;

import java.util.List;

public interface RuleProvider {

  /**
   * It will return a list of pre-defined rules. If it does not have, it will return a empty list.
   * 
   * @return
   */
  public List<RuntimeRule> getDefinedRules();
  
}
