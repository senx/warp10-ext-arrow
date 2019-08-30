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

package arrow.warpscriptFunctions;

import arrow.ArrowAdapterHelper;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.formatted.FormattedWarpScriptFunction;
import org.jamon.annotations.Argument;

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

    getDocstring().append("TODO");

    args =  new ArgumentsBuilder()
      .addArgument(Object.class, IN, "The object to be converted.")
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

      stack.push(ArrowAdapterHelper.gtstoArrowStream(gts, nTicksPerBatch));

    } else if (in instanceof GTSEncoder) {

      throw new WarpScriptException(getName() + "TODO");

    } else if (in instanceof List) {

      throw new WarpScriptException(getName() + ": TODO");

    } else if (in instanceof Map) {

      throw new WarpScriptException(getName() + ": TODO");

    } else {

      throw  new WarpScriptException(getName() + ": unsupported input type.");
    }

    return stack;
  }
}
