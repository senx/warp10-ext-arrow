//
//   Copyright 2020  SenX S.A.S.
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

import io.warp10.arrow.direct.ArrowWriters;
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

    getDocstring().append("Encode input into Arrow columnar format (BYTES). The conversion table is in the extension's README.");

    args =  new ArgumentsBuilder()
      .addArgument(Object.class, IN, "See the README of the extension for supported types.")
      .build();

    output = new ArgumentsBuilder()
      .addArgument(byte[].class, OUT, "The resulting byte array.")
      .build();
  }

  @Override
  public WarpScriptStack apply(Map<String, Object> params, WarpScriptStack stack) throws WarpScriptException {
    Object in = params.get(IN);

    if (in instanceof GeoTimeSerie) {

      //
      // type GTS
      //

      GeoTimeSerie gts = (GeoTimeSerie) in;
      if (0 == gts.size()) {
        throw new WarpScriptException("Input is empty.");
      }

      ByteArrayOutputStream out =  new ByteArrayOutputStream();
      ArrowWriters.gtsToArrowStream(gts, out);
      stack.push(out.toByteArray());

    } else if (in instanceof GTSEncoder) {

      throw new WarpScriptException("GTSENCODER input type is not supported anymore since 2.0.0. Use instead a list of GTSENCODER.");

    } else if (in instanceof List) {

      List list = (List) in;
      if (0 == list.size()) {
        throw new WarpScriptException(getName() + " received an empty list.");
      }

      if (list.get(0) instanceof Map) {

        //
        // type PAIR: list containing map of metadata and map of list
        //

        if (2 != list.size() || !(list.get(0) instanceof Map) || !(list.get(1) instanceof Map)) {
          throw new WarpScriptException("When " + getName() + "'s input is a pair LIST of Map, it expects two items: custom metadata (a MAP), and columns (a MAP of LIST of same size).");
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
              throw new WarpScriptException(getName() + ": incoherent column sizes. They must be equal.");
            }
          }
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowWriters.columnsToArrowStream(list, commonSize, out);
        stack.push(out.toByteArray());

      } else {

        //
        // type ENCODERS: a list of GTSENCODERS (and/or GTS)
        //

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
