//
// Copyright 2020 SenX
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

package io.warp10.arrow.direct;

import io.warp10.arrow.warpscriptFunctions.ARROWTO;
import io.warp10.script.functions.TYPEOF;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Utilities and converters
 *
 * TODO(JC): maybe refactor into multiple classes (one reader/writer per subtype and type).
 */
public class ArrowHelper {

  public final static String TIMESTAMPS_KEY = "timestamp";
  public final static String LONG_VALUES_KEY = TYPEOF.typeof(Long.class);
  public final static String DOUBLE_VALUES_KEY = TYPEOF.typeof(Double.class);
  //public final static String BIGDECIMAL_VALUES_KEY = "BIGDECIMAL.CONTENT";
  //public final static String BIGDECIMAL_SCALES_KEY = "BIGDECIMAL.SCALE";
  public final static String BOOLEAN_VALUES_KEY = TYPEOF.typeof(Boolean.class);
  public final static String STRING_VALUES_KEY = TYPEOF.typeof(String.class);
  public final static String BYTES_VALUES_KEY = TYPEOF.typeof(byte[].class);
  public final static String LATITUDE_KEY = "latitude";
  public final static String LONGITUDE_KEY = "longitude";
  public final static String ELEVATION_KEY = "elevation";

  public final static String BUCKETSPAN = "bucketspan";
  public final static String BUCKETCOUNT = "bucketcount";
  public final static String LASTBUCKET = "lastbucket";

  public final static String MODE = ARROWTO.MODE;
  public final static String REV = "WarpScriptVersion";
  public final static String STU = "WarpScriptTimeUnitsPerSecond";

  //
  // Fields of arrow schemas
  // Except for timestamp field, we make them nullable so we can use them for GTSEncoders
  //

  private static Field nonNullable(String key, ArrowType type) {
    return new Field(key, new FieldType(false, type, null), null);
  }

  // GTS fields
  final static Field TIMESTAMP_FIELD = nonNullable(TIMESTAMPS_KEY, new ArrowType.Int(64, true));
  final static Field LONG_VALUES_FIELD = Field.nullable(LONG_VALUES_KEY,new ArrowType.Int(64, true));
  final static Field DOUBLE_VALUES_FIELD = Field.nullable(DOUBLE_VALUES_KEY, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
  final static Field BOOLEAN_VALUES_FIELD = Field.nullable(BOOLEAN_VALUES_KEY, new ArrowType.Bool());
  final static Field STRING_VALUES_FIELD = Field.nullable(STRING_VALUES_KEY, new ArrowType.Utf8());
  final static Field LATITUDE_FIELD = Field.nullable(LATITUDE_KEY, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
  final static Field LONGITUDE_FIELD = Field.nullable(LONGITUDE_KEY, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
  final static Field ELEVATION_FIELD = Field.nullable(ELEVATION_KEY, new ArrowType.Int(64, true));

  // additional fields for GTSEncoders
  final static Field BYTES_VALUES_FIELD = Field.nullable(BYTES_VALUES_KEY, new ArrowType.Binary());

}
