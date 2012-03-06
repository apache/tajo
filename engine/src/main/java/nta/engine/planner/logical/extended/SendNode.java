/**
 * 
 */
package nta.engine.planner.logical.extended;

import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import nta.engine.json.GsonCreator;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.UnaryNode;
import nta.engine.utils.TUtil;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * This logical node means that the worker sends intermediate data to 
 * some destined one or more workers.
 * 
 * @author Hyunsik Choi
 */
public class SendNode extends UnaryNode {
  @Expose private PipeType pipeType;
  @Expose private RepartitionType repaType;
  /** This will be used for pipeType == PUSH. */
  @Expose private Map<Integer, URI> destURIs;

  private SendNode() {
    super(ExprType.SEND);
  }
  
  public SendNode(PipeType pipeType, RepartitionType repaType) {
    this();
    this.pipeType = pipeType;
    this.repaType = repaType;
    this.destURIs = Maps.newHashMap();
  }

  public PipeType getPipeType() {
    return this.pipeType;
  }
  
  public RepartitionType getRepartitionType() {
    return this.repaType;
  }
  
  public URI getDestURI(int partition) {
    return this.destURIs.get(partition);
  }
  
  public Iterator<Entry<Integer, URI>> getAllDestURIs() {
    return this.destURIs.entrySet().iterator();
  }
  
  public void putDestURI(int partition, URI uri) {
    this.destURIs.put(partition, uri);
  }
  
  public void setDestURIs(Map<Integer, URI> destURIs) {
    this.destURIs = destURIs;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SendNode) {
      SendNode other = (SendNode) obj;
      return pipeType == other.pipeType
          && repaType == other.repaType
          && TUtil.checkEquals(destURIs, other.destURIs);
    } else {
      return false;
    }
  }
  
  @Override
  public int hashCode() {
    return Objects.hashCode(pipeType, repaType, destURIs);
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    SendNode send = (SendNode) super.clone();
    send.pipeType = pipeType;
    send.repaType = repaType;
    send.destURIs = Maps.newHashMap();
    for (Entry<Integer, URI> entry : destURIs.entrySet()) {
      send.destURIs.put((int)entry.getKey(), entry.getValue());
    }
    
    return send;
  }

  @Override
  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(this);
  }

  @Override
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}