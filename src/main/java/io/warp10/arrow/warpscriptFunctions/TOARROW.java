//
//   Copyright 2019  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.arrow.warpscriptFunctions;

import io.warp10.arrow.direct.ArrowVectorHelper;
import io.warp10.arrow.pojo.WarpSchema;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.formatted.FormattedWarpScriptFunction;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

/**
 * Encode an Object as an Arrow stream
 */
public class TOARROW extends FormattedWarpScriptFunction {

  private static final String IN = "in";
  private static final String BATCH_SIZE = "nTicksPerBatch";
  private static final String OUT = "out";

  private final Arguments args;
  private final Arguments output;

  public Arguments getArguments() {
    return args;
  }
  public Arguments getOutput() {
    return output;
  }

  public TOARROW(String name) {
    super(name);

    getDocstring().append("Conversion to Apache Arrow [streaming format](https://arrow.apache.org/docs/ipc.html).\n" +
      "\n" +
      "Arrow format is tabular and is composed of a set of fields (i.e. named columns). It can also be associated with a map of metadata.\n" +
      "The output has at least a `classname` field, and a `timestamp` field. Then depending if they would not be empty, it can also have one field per GTS label or attribute key found in the input data, a `latitude` field, a `longitude` field, an `elevation` field, and one field per value type: `LONG`, `DOUBLE`, `BOOLEAN`, `STRING`, `BYTES`.\n" +
      "\n" +
      "Note that data points from different input GTS or GTSENCODER that are of the same type are in the same column. They can be identified by the `classname` and label related fields.");

    args =  new ArgumentsBuilder()
      .addArgument(Object.class, IN, "A LIST containing GTS or GTSENCODER instances. For other supported types, see the README of the warp10-ext-arrow extension.")
      .addOptionalArgument(Long.class, BATCH_SIZE, "The number of data point per batch. Default to full size.", 0L)
      .build();

    output = new ArgumentsBuilder()
      .addArgument(byte[].class, OUT, "The resulting byte array.")
      .build();
  }

  @Override
  public WarpScriptStack apply(Map<String, Object> params, WarpScriptStack stack) throws WarpScriptException {
    Object in = params.get(IN);
    int nTicksPerBatch = ((Long) params.get(BATCH_SIZE)).intValue();

    if (in instanceof GeoTimeSerie) {

      GeoTimeSerie gts = (GeoTimeSerie) in;
      if (0 == nTicksPerBatch) {
        nTicksPerBatch = gts.size();
      }

      ByteArrayOutputStream out =  new ByteArrayOutputStream();
      ArrowVectorHelper.gtstoArrowStream(gts, nTicksPerBatch, out);
      stack.push(out.toByteArray());

    } else if (in instanceof GTSEncoder) {

      GTSEncoder encoder = (GTSEncoder) in;
      if (0 == nTicksPerBatch) {
        nTicksPerBatch = (int) encoder.getCount();
      }

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ArrowVectorHelper.gtsEncodertoArrowStream(encoder, nTicksPerBatch, out);
      stack.push(out.toByteArray());

    } else if (in instanceof List) {

      List list = (List) in;
      if (0 == list.size()) {
        throw new WarpScriptException(getName() + " received an empty list.");
      }

      if (list.get(0) instanceof Map) {

        //
        // The most generic behaviour
        //

        if (2 != list.size() || !(list.get(0) instanceof Map) || !(list.get(1) instanceof Map)) {
          throw new WarpScriptException("When " + getName() + "'s input is a pair LIST of Map, it expects two items: custom metadata (a MAP), and field vectors (a MAP of LIST of same size).");
        }

        Map<String, List> columns = (Map<String, List>) list.get(1);

        Integer commonSize = null;
        for (String key : columns.keySet()) {
          if (0 == columns.get(key).size()) {
            continue;
          }

          if (null == commonSize) {
            commonSize = columns.get(key).size();
          } else {
            if (commonSize != columns.get(key).size()) {
              throw new WarpScriptException(getName() + ": incoherent field vector size. They must be equal.");
            }
          }
        }

        if (0 == nTicksPerBatch) {
          nTicksPerBatch = commonSize;
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowVectorHelper.columnsToArrowStream(list, nTicksPerBatch, out);
        stack.push(out.toByteArray());

      } else {

        //
        // This is the main behaviour
        //

        if (0 != nTicksPerBatch) {
          throw new WarpScriptException(BATCH_SIZE + " argument is unused for input of this type. It should not be set.");
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        WarpSchema.GtsOrEncoderListSchema(list).writeListToStream(out, list);
        stack.push(out.toByteArray());
      }

    } else {

      throw  new WarpScriptException(getName() + ": unsupported input type.");
    }

    return stack;
  }
}
