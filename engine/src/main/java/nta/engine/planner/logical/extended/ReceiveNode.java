/**
 * 
 */
package nta.engine.planner.logical.extended;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import nta.engine.json.GsonCreator;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalNodeVisitor;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 */
public final class ReceiveNode extends LogicalNode implements Cloneable {
  @Expose private PipeType pipeType;
  @Expose private RepartitionType repaType;
  @Expose private Map<String, List<URI>> fetchMap;

  private ReceiveNode() {
    super(ExprType.RECEIVE);
  }
  public ReceiveNode(PipeType pipeType, RepartitionType shuffleType) {
    this();
    this.pipeType = pipeType;
    this.repaType = shuffleType;
    this.fetchMap = Maps.newHashMap();
  }

  public PipeType getPipeType() {
    return this.pipeType;
  }

  public RepartitionType getRepartitionType() {
    return this.repaType;
  }
  
  public void addData(String name, URI uri) {
    if (fetchMap.containsKey(name)) {
      fetchMap.get(name).add(uri);
    } else {
      fetchMap.put(name, Lists.newArrayList(uri));
    }
  }
  
  public Collection<URI> getSrcURIs(String name) {
    return Collections.unmodifiableList(fetchMap.get(name));
  }

  public Iterator<Entry<String, List<URI>>> getAllDataSet() {
    return fetchMap.entrySet().iterator();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ReceiveNode) {
      ReceiveNode other = (ReceiveNode) obj;
      return pipeType == other.pipeType && repaType == other.repaType;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(pipeType, repaType);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    ReceiveNode receive = (ReceiveNode) super.clone();
    receive.pipeType = pipeType;
    receive.repaType = repaType;
    receive.fetchMap = Maps.newHashMap();
    // Both String and URI are immutable, but a list is mutable.
    for (Entry<String, List<URI>> entry : fetchMap.entrySet()) {
      receive.fetchMap
          .put(entry.getKey(), new ArrayList<URI>(entry.getValue()));
    }

    return receive;
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

  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }
  
  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }
}
