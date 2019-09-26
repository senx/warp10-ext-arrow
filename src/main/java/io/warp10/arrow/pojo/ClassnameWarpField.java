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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class ClassnameWarpField extends DictionaryEncodedWarpField {

  static final String CLASSNAME_KEY = "classname";
  private static final ArrowType.Int INDEX_TYPE = new ArrowType.Int(16, false); // max 2¹⁶ classes
  private static final DictionaryEncoding DICTIONARY_ENCODING = new DictionaryEncoding(0, false, INDEX_TYPE);
  private static final Field INDEX_FIELD = new Field(CLASSNAME_KEY, new FieldType(true, INDEX_TYPE, DICTIONARY_ENCODING), null);
  private static final Field DICTIONARY_FIELD = Field.nullable(CLASSNAME_KEY + "::dictionary", new ArrowType.Utf8());

  public ClassnameWarpField(){}
  public ClassnameWarpField(BufferAllocator allocator) {
    super(allocator);
  }

  public String getKey() {
    return CLASSNAME_KEY;
  }

  public Field getField() {
    return INDEX_FIELD;
  }

  public String getWarpScriptType() {
    return TYPEOF.typeof(String.class);
  }

  public Field getDictionaryField() {
    return DICTIONARY_FIELD;
  }

  public DictionaryEncoding getDictionaryEncoding() {
    return DICTIONARY_ENCODING;
  }

  @Override
  public void setSafe(int index, Object o) {
    super.setSafe(index, o);
  }
}
