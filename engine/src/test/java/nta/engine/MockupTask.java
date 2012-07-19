package nta.engine;

/**
 *
 * @author jihoon
 */
public class MockupTask {
  private QueryUnitId unitId;
  private MasterInterfaceProtos.QueryStatus status;
  private int leftTime;

  public MockupTask(QueryUnitId unitId,
                    int runTime) {
    this.unitId = unitId;
    this.status = MasterInterfaceProtos.QueryStatus.QUERY_INPROGRESS;
    this.leftTime = runTime;
  }

  public QueryUnitId getId() {
    return this.unitId;
  }

  public MasterInterfaceProtos.QueryStatus getStatus() {
    return this.status;
  }

  public int getLeftTime() {
    return this.leftTime;
  }

  public void updateTime(int time) {
    this.leftTime -= time;
  }

  public void setStatus(MasterInterfaceProtos.QueryStatus status) {
    this.status = status;
  }
}
