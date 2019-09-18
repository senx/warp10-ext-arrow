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

import io.warp10.arrow.ArrowVectorHelper;
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

  public TOARROW(String name) {
    super(name);

    getDocstring().append("Encode an object into a byte array in Arrow streaming format.");

    args =  new ArgumentsBuilder()
      .addArgument(Object.class, IN, "The object to be converted: GTS, GTSENCODER, or a LIST of two items: custom metadata (a MAP), and field vectors (a MAP of LIST of same size).")
      .addOptionalArgument(Long.class, BATCH_SIZE, "The number of data point per batch. Default to full size.", 0L)
      .build();

    output = new ArgumentsBuilder()
      .addArgument(byte[].class, OUT, "The resulting byte stream.")
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

      List pair = (List) in;
      if (2 != pair.size() || !(pair.get(0) instanceof Map) || !(pair.get(1) instanceof Map)) {
        throw new WarpScriptException("When " + getName() + "'s input is a LIST, it expects two items: custom metadata (a MAP), and field vectors (a MAP of LIST of same size).");
      }

      Map<String, List> columns = (Map<String, List>) pair.get(1);

      Integer commonSize = null;
      for (String key: columns.keySet()) {
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
      ArrowVectorHelper.columnsToArrowStream(pair, nTicksPerBatch, out);
      stack.push(out.toByteArray());

    } else {

      throw  new WarpScriptException(getName() + ": unsupported input type.");
    }

    return stack;
  }
}
