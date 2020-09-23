//
// Copyright 2019 SenX
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package io.warp10.arrow.pojo;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * A WarpField object holds every Arrow's object related to a particular WarpScript object
 */
public abstract class WarpField {

  private FieldVector vector;

  public WarpField(){}

  public WarpField(BufferAllocator allocator) {
    initialize(allocator);
  }

  public void initialize(BufferAllocator allocator) {
    if (null != vector) {
      throw new RuntimeException("Field has already been initialized.");
    }

    vector = getField().createVector(allocator);
  }

  final public FieldVector getVector() {
    if (null == vector) {
      throw new RuntimeException("Field has not been initialized yet.");
    }
    return vector;
  }

  public abstract String getKey();
  public abstract Field getField();
  public abstract String getWarpScriptType();
  public abstract void setSafe(int index, Object o);

  public void setSafeUpdateValueCount(int index, Object o) {
    setSafe(index, o);

    if (getVector().getValueCount() <= index) {
      getVector().setValueCount(index + 1);
    }
  }

  public void addValue(Object o) {
    setSafeUpdateValueCount(getValueCount(), o);
  }
  
  public void padWithNull(int valueCount) {
    if (valueCount > getValueCount()) {
      setSafeUpdateValueCount(valueCount - 1, null);
    }
  }

  public int getValueCount() {
    return getVector().getValueCount();
  }

  protected void clear() {
    getVector().clear();
  }

  final protected static Field nonNullable(String key, ArrowType type) {
    return new Field(key, new FieldType(false, type, null), null);
  }

  public Object get(int index) {
    if (getVector().isNull(index)) {
      return null;
    }

    return getVector().getObject(index);
  }

}
