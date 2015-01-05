/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.logical;

import com.google.gson.annotations.Expose;
import org.apache.tajo.json.GsonObject;

public abstract class BinaryNode extends LogicalNode implements Cloneable, GsonObject {
	@Expose LogicalNode leftChild = null;
	@Expose LogicalNode rightChild = null;

	public BinaryNode(int pid, NodeType nodeType) {
		super(pid, nodeType);
	}

  @Override
  public int childNum() {
    return 2;
  }

  @Override
  public LogicalNode getChild(int idx) {
    if (idx == 0) {
      return leftChild;
    } else if (idx == 1) {
      return rightChild;
    } else {
      throw new ArrayIndexOutOfBoundsException(idx);
    }
  }
	
	public <T extends LogicalNode> T getLeftChild() {
		return (T) this.leftChild;
	}
	
	public void setLeftChild(LogicalNode op) {
		this.leftChild = op;
	}

	public <T extends LogicalNode> T getRightChild() {
		return (T) this.rightChild;
	}

	public void setRightChild(LogicalNode op) {
		this.rightChild = op;
	}

  public boolean deepEquals(Object o) {
    if (o instanceof BinaryNode) {
      BinaryNode b = (BinaryNode) o;
      return equals(o) &&
          leftChild.deepEquals(b.leftChild) && rightChild.deepEquals(b.rightChild);
    }
    return false;
  }
	
	@Override
  public Object clone() throws CloneNotSupportedException {
	  BinaryNode binNode = (BinaryNode) super.clone();
	  binNode.leftChild = (LogicalNode) leftChild.clone();
	  binNode.rightChild = (LogicalNode) rightChild.clone();
	  
	  return binNode;
	}
	
	public void preOrder(LogicalNodeVisitor visitor) {
	  visitor.visit(this);
	  leftChild.postOrder(visitor);
    rightChild.postOrder(visitor);
  }
	
	public void postOrder(LogicalNodeVisitor visitor) {
    leftChild.postOrder(visitor);
    rightChild.postOrder(visitor);
    visitor.visit(this);
  }
}
