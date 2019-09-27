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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

public class TimestampWarpField extends WarpField {

  public static final String TIMESTAMPS_KEY = "timestamp";
  private static final Field TIMESTAMP_FIELD = nonNullable(TIMESTAMPS_KEY, new ArrowType.Int(64, true));

  public TimestampWarpField(){}

  public TimestampWarpField(BufferAllocator allocator) {
    super(allocator);
  }

  public String getKey() {
    return TIMESTAMPS_KEY;
  }

  public Field getField() {
    return TIMESTAMP_FIELD;
  }

  public String getWarpScriptType() {
    return TYPEOF.typeof(Long.class);
  }

  public void setSafe(int index, Object o) {

    if (null == o) return;

    if (!(o instanceof Long)) {
      throw new RuntimeException(getField() + " field expect to set input of type Long.");
    }

    ((BigIntVector)  getVector()).setSafe(index, (long) o);
  }
}
