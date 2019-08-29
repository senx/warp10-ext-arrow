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
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.formatted.FormattedWarpScriptFunction;

import java.util.Map;

/**
 * Encode a GTS as an Arrow stream
 */
public class TOARROW extends FormattedWarpScriptFunction {

  private static final String GTS = "gts";
  private static final String BATCH_SIZE = "nTicksPerBatch";

  private final Arguments args;

  public Arguments getArguments() {
    return args;
  }

  public TOARROW(String name) {
    super(name);

    getDocstring().append("TODO");

    args =  new ArgumentsBuilder()
      .addArgument(GeoTimeSerie.class, GTS, "The Geo Time Series to be converted.") //TODO(JC): do it for GTSencoders as well
      //.firstArgIsListExpandable()
      .addOptionalArgument(Long.class, BATCH_SIZE, "The number of ticks per batch. Default to gts size.", 0L)
      .build();

  }

  @Override
  public WarpScriptStack apply(Map<String, Object> params, WarpScriptStack stack) throws WarpScriptException {
    GeoTimeSerie gts = (GeoTimeSerie) params.get(GTS);

    int nTicksPerBatch = ((Long) params.get(BATCH_SIZE)).intValue();
    if (0 == nTicksPerBatch) {
      nTicksPerBatch = gts.size();
    }

    stack.push(ArrowAdapterHelper.gtstoArrowStream(gts, nTicksPerBatch));
    return stack;
  }
}
