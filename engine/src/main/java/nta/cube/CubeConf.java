package nta.cube;

import nta.catalog.Schema;

/* Cube configure 클래스 */
public class CubeConf {
  private int nodenum;
  private Schema inschema;
  private Schema outschema;
  private String localInput;
  private String globalOutput;

  public CubeConf() {
  }

  public void setNodenum(int nodenum) {
    this.nodenum = nodenum;
  }

  public int getNodenum() {
    return nodenum;
  }

  public Schema getInschema() {
    return inschema;
  }

  public void setInschema(Schema inschema) {
    this.inschema = inschema;
  }

  public Schema getOutschema() {
    return outschema;
  }

  public void setOutschema(Schema outschema) {
    this.outschema = outschema;
  }

  public String getLocalInput() {
    return localInput;
  }

  public void setLocalInput(String localInput) {
    this.localInput = localInput;
  }

  public String getGlobalOutput() {
    return globalOutput;
  }

  public void setGlobalOutput(String globalOutput) {
    this.globalOutput = globalOutput;
  }

}
