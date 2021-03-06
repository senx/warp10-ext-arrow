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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A WarpField of Strings that is dictionary encoded
 */
public abstract class DictionaryEncodedWarpField extends WarpField {

  public DictionaryEncodedWarpField(){}
  public DictionaryEncodedWarpField(BufferAllocator allocator) {
    super(allocator);
  }
  public DictionaryEncodedWarpField(List<Object> initialDictionary){
    this.initialDictionary  = initialDictionary;
  }
  public DictionaryEncodedWarpField(BufferAllocator allocator, List<Object> initialDictionary) {
    super(allocator);

    this.initialDictionary = initialDictionary;
  }

  private Dictionary dictionary;
  private Map<Object, Integer> lookUps;
  private List<Object> initialDictionary;

  public abstract Field getDictionaryField();
  public abstract DictionaryEncoding getDictionaryEncoding();

  public void initialize(BufferAllocator allocator) {
    super.initialize(allocator);

    if (null != dictionary) {
      throw new RuntimeException("Dictionary field has already been initialized.");
    }

    if (!getDictionaryEncoding().getIndexType().getIsSigned()) {
      throw new RuntimeException("Unsigned int not supported.");
    }

    dictionary = new Dictionary(getDictionaryField().createVector(allocator), getDictionaryEncoding());
    lookUps = new HashMap<Object, Integer>();

    if (null != initialDictionary) {
      int i = 0;
      for (Object o: initialDictionary) {
        lookUps.put(o, i);

        VarCharVector dictionaryVector = ((VarCharVector)  getDictionaryVector());
        dictionaryVector.setSafe(i, new Text((String) o));
        dictionaryVector.setValueCount(dictionaryVector.getValueCount() + 1);
        i++;
      }

    } else {

      // @see https://github.com/apache/arrow/issues/5527
      throw new RuntimeException("The dictionary must be provided before reading data due to a bug in current java implementation of Arrow library.");
    }
  }

  final protected Dictionary getDictionary() {
    if (null == dictionary) {
      throw new RuntimeException("Dictionary field has not been initialized yet.");
    }

    return dictionary;
  }

  final public long getDictionaryId() {
    return getDictionaryEncoding().getId();
  }

  final protected FieldVector getDictionaryVector() {
    return getDictionary().getVector();
  }

  protected void clearDictionary() {
    getDictionaryVector().clear();
    lookUps.clear();
  }

  public void setSafe(int index, Object o) {

    if (null == o) {
      switch (getDictionaryEncoding().getIndexType().getBitWidth()) {
        case 8:
          ((TinyIntVector) getVector()).setNull(index);
          break;

        case 16:
          ((SmallIntVector) getVector()).setNull(index);
          break;

        case 32:
          ((IntVector) getVector()).setNull(index);
          break;

        case 64:
        default:
          throw new RuntimeException("Index type with bit width other than 8, 16 ot 32 are not supported.");
      }

      return;
    }

    if (!(o instanceof String)) {
      throw new RuntimeException(getField() + " field expect to set input of type String.");
    }

    Integer id = lookUps.get(o);

    if (null == id) {
      id = lookUps.size();
      lookUps.put(o, lookUps.size());

      VarCharVector dictionaryVector = ((VarCharVector)  getDictionaryVector());
      dictionaryVector.setSafe(id, new Text((String) o));
      dictionaryVector.setValueCount(dictionaryVector.getValueCount() + 1);

      // Right now we can not continue
      // @see https://github.com/apache/arrow/issues/5527
      throw new RuntimeException("The dictionary must be provided before reading data due to a bug in current java implementation of Arrow library.");
    }

    switch (getDictionaryEncoding().getIndexType().getBitWidth()) {
      case 8:
        ((TinyIntVector) getVector()).setSafe(index, id);
        break;

      case 16:
        ((SmallIntVector) getVector()).setSafe(index, id.shortValue());
        break;

      case 32:
        ((IntVector) getVector()).setSafe(index, id);
        break;

      case 64:
      default:
        throw new RuntimeException("Index type with bit width other than 8, 16 ot 32 are not supported.");
    }
  }
}
