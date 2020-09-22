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

import io.warp10.script.functions.TYPEOF;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

public class LatitudeWarpField extends WarpField {

  public static final String LATITUDE_KEY = "latitude";
  private static final Field LATITUDE_FIELD = Field.nullable(LATITUDE_KEY, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));

  public LatitudeWarpField(){}

  public LatitudeWarpField(BufferAllocator allocator) {
    super(allocator);
  }

  public String getKey() {
    return LATITUDE_KEY;
  }

  public Field getField() {
    return LATITUDE_FIELD;
  }

  public String getWarpScriptType() {
    return TYPEOF.typeof(Double.class);
  }

  public void setSafe(int index, Object o) {

    if (null == o) {
      ((Float4Vector)  getVector()).setNull(index);
      return;
    }

    if (!(o instanceof Double)) {
      throw new RuntimeException(getField() + " field expect to set input of type double.");
    }

    ((Float4Vector)  getVector()).setSafe(index, ((Double) o).floatValue()); // actually cast it to single precision
  }

  @Override
  public Object get(int index) {
    return getDouble(index);
  }

  public Double getDouble(int index) {
    if (getVector().isNull(index)) {
      return null;
    }

    return new Float(((Float4Vector)  getVector()).get(index)).doubleValue();
  }
}
