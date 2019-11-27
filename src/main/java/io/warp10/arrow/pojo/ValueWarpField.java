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
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

public class ValueWarpField extends WarpField {

  public final static String LONG_VALUES_KEY = TYPEOF.typeof(Long.class);
  public final static String DOUBLE_VALUES_KEY = TYPEOF.typeof(Double.class);
  public final static String BOOLEAN_VALUES_KEY = TYPEOF.typeof(Boolean.class);
  public final static String STRING_VALUES_KEY = TYPEOF.typeof(String.class);
  public final static String BYTES_VALUES_KEY = TYPEOF.typeof(byte[].class); // for GtsEncoder

  private final static Field LONG_VALUES_FIELD = Field.nullable(LONG_VALUES_KEY,new ArrowType.Int(64, true));
  private final static Field DOUBLE_VALUES_FIELD = Field.nullable(DOUBLE_VALUES_KEY, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
  private final static Field BOOLEAN_VALUES_FIELD = Field.nullable(BOOLEAN_VALUES_KEY, new ArrowType.Bool());
  private final static Field STRING_VALUES_FIELD = Field.nullable(STRING_VALUES_KEY, new ArrowType.Utf8());
  private final static Field BYTES_VALUES_FIELD = Field.nullable(BYTES_VALUES_KEY, new ArrowType.Binary());

  public enum Type {
    LONG(Long.class),
    DOUBLE(Double.class),
    BOOLEAN(Boolean.class),
    STRING(String.class),
    BYTES(byte[].class);

    private final Class<?> clazz;
    public Class<?> getCorrespondingClass() {
      return clazz;
    }
    Type(Class<?> clazz){
      this.clazz = clazz;
    }
  }

  public final Type type;

  public ValueWarpField(Type type) {
    this.type = type;
  }

  public ValueWarpField(BufferAllocator allocator, Type type) {
    super(allocator);
    this.type = type;
  }

  public String getKey() {

    switch (type) {
      case LONG:
        return LONG_VALUES_KEY;

      case DOUBLE:
        return DOUBLE_VALUES_KEY;

      case BOOLEAN:
        return BOOLEAN_VALUES_KEY;

      case STRING:
        return STRING_VALUES_KEY;

      case BYTES:
        return BYTES_VALUES_KEY;

      default:
        throw new RuntimeException("Unrecognized type.");
    }
  }

  public Field getField() {

    switch (type) {
      case LONG:
        return LONG_VALUES_FIELD;

      case DOUBLE:
        return DOUBLE_VALUES_FIELD;

      case BOOLEAN:
        return BOOLEAN_VALUES_FIELD;

      case STRING:
        return STRING_VALUES_FIELD;

      case BYTES:
        return BYTES_VALUES_FIELD;

      default:
        throw new RuntimeException("Unrecognized type.");
    }
  }

  public String getWarpScriptType() {
    return TYPEOF.typeof(type.getCorrespondingClass());
  }

  public Type getType() {
    return type;
  }

  private void setSafeLong(int index, Object o) {

    if (null == o) {
      ((BigIntVector)  getVector()).setNull(index);
      return;
    }

    if (!(o instanceof Long)) {
      throw new RuntimeException(getField() + " field expect to set input of type long.");
    }

    ((BigIntVector)  getVector()).setSafe(index, (long) o);
  }

  private void setSafeDouble(int index, Object o) {

    if (null == o) {
      ((Float8Vector)  getVector()).setNull(index);
      return;
    }

    if (!(o instanceof Double)) {
      throw new RuntimeException(getField() + " field expect to set input of type double.");
    }

    ((Float8Vector)  getVector()).setSafe(index, (double) o);
  }

  private void setSafeBoolean(int index, Object o) {

    if (null == o) {
      ((BitVector)  getVector()).setNull(index);
      return;
    }

    if (!(o instanceof Boolean)) {
      throw new RuntimeException(getField() + " field expect to set input of type bolean.");
    }

    ((BitVector)  getVector()).setSafe(index, (boolean) o ? 1 : 0);
  }

  private void setSafeString(int index, Object o) {

    if (null == o) {
      ((VarCharVector)  getVector()).setNull(index);
      return;
    }

    if (!(o instanceof String)) {
      throw new RuntimeException(getField() + " field expect to set input of type String.");
    }

    ((VarCharVector)  getVector()).setSafe(index, new Text((String) o));
  }

  private void setSafeBytes(int index, Object o) {

    if (null == o) {
      ((VarBinaryVector)  getVector()).setNull(index);
      return;
    }

    if (!(o instanceof byte[])) {
      throw new RuntimeException(getField() + " field expect to set input of type byte[].");
    }

    ((VarBinaryVector)  getVector()).setSafe(index, (byte[]) o);
  }

  public void setSafe(int index, Object o) {

    switch (type) {
      case LONG:
        setSafeLong(index, o);
        break;

      case DOUBLE:
        setSafeDouble(index, o);
        break;

      case BOOLEAN:
        setSafeBoolean(index, o);
        break;

      case STRING:
        setSafeString(index, o);
        break;

      case BYTES:
        setSafeBytes(index, o);
        break;

      default:
        throw new RuntimeException("Unrecognized type.");
    }
  }
}
