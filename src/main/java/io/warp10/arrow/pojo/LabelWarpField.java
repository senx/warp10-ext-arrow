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

import java.util.ArrayList;
import java.util.List;

public class LabelWarpField extends DictionaryEncodedWarpField {

  private final String labelKey;
  private static final ArrowType.Int INDEX_TYPE = new ArrowType.Int(16, true); // max 2¹⁵ label keys
  private final DictionaryEncoding encoding;
  private final Field indexField;
  private final Field dictionaryField;

  public enum Type {
    LABEL,
    ATTRIBUTE
  }
  private final Type type;

  private final static List<String> reservedFieldNames = new ArrayList<>(); // names that can't be used as key of label or attributes
  static {
    reservedFieldNames.add(TimestampWarpField.TIMESTAMPS_KEY);
    reservedFieldNames.add(LatitudeWarpField.LATITUDE_KEY);
    reservedFieldNames.add(LongitudeWarpField.LONGITUDE_KEY);
    reservedFieldNames.add(ElevationWarpField.ELEVATION_KEY);
    reservedFieldNames.add(ValueWarpField.LONG_VALUES_KEY);
    reservedFieldNames.add(ValueWarpField.DOUBLE_VALUES_KEY);
    reservedFieldNames.add(ValueWarpField.BOOLEAN_VALUES_KEY);
    reservedFieldNames.add(ValueWarpField.STRING_VALUES_KEY);
    reservedFieldNames.add(ValueWarpField.BYTES_VALUES_KEY);
    reservedFieldNames.add(ClassnameWarpField.CLASSNAME_KEY);
  }

  public LabelWarpField(String labelKey, int dictionaryId, Type type){
    if (reservedFieldNames.contains(labelKey)) {
      throw new RuntimeException("Label key '" + labelKey + "' is reserved by serialization process. Please rename it.");
    }

    this.labelKey = labelKey;
    encoding = new DictionaryEncoding(dictionaryId, false, INDEX_TYPE);
    indexField = new Field(labelKey, new FieldType(true, INDEX_TYPE, encoding), null);
    dictionaryField = Field.nullable(labelKey + "::dictionary", new ArrowType.Utf8());
    this.type = type;
  }

  public LabelWarpField(BufferAllocator allocator, String labelKey, int dictionaryId, Type type) {
    super(allocator);

    if (reservedFieldNames.contains(labelKey)) {
      throw new RuntimeException("Label key '" + labelKey + "' is reserved by serialization process. Please rename it.");
    }

    this.labelKey = labelKey;
    encoding = new DictionaryEncoding(dictionaryId, false, INDEX_TYPE);
    indexField = new Field(labelKey, new FieldType(true, INDEX_TYPE, encoding), null);
    dictionaryField = Field.nullable(labelKey + "::dictionary", new ArrowType.Utf8());
    this.type = type;
  }

  public LabelWarpField(String labelKey, int dictionaryId, Type type, List<Object> initialDictionary){
    super(initialDictionary);

    if (reservedFieldNames.contains(labelKey)) {
      throw new RuntimeException("Label key '" + labelKey + "' is reserved by serialization process. Please rename it.");
    }

    this.labelKey = labelKey;
    encoding = new DictionaryEncoding(dictionaryId, false, INDEX_TYPE);
    indexField = new Field(labelKey, new FieldType(true, INDEX_TYPE, encoding), null);
    dictionaryField = Field.nullable(labelKey + "::dictionary", new ArrowType.Utf8());
    this.type = type;
  }

  public LabelWarpField(BufferAllocator allocator, String labelKey, int dictionaryId, Type type, List<Object> initialDictionary) {
    super(allocator, initialDictionary);

    if (reservedFieldNames.contains(labelKey)) {
      throw new RuntimeException("Label key '" + labelKey + "' is reserved by serialization process. Please rename it.");
    }

    this.labelKey = labelKey;
    encoding = new DictionaryEncoding(dictionaryId, false, INDEX_TYPE);
    indexField = new Field(labelKey, new FieldType(true, INDEX_TYPE, encoding), null);
    dictionaryField = Field.nullable(labelKey + "::dictionary", new ArrowType.Utf8());
    this.type = type;
  }

  public String getKey() {
    return labelKey;
  }

  public Field getField() {
    return indexField;
  }

  public String getWarpScriptType() {
    return TYPEOF.typeof(String.class);
  }

  public Field getDictionaryField() {
    return dictionaryField;
  }

  public DictionaryEncoding getDictionaryEncoding() {
    return encoding;
  }

  public Type getType() { return type; }

  @Override
  public void setSafe(int index, Object o) {
    super.setSafe(index, o);
  }
}
