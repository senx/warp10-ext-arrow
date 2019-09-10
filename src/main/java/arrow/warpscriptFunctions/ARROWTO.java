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

package arrow.warpscriptFunctions;

import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.formatted.FormattedWarpScriptFunction;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.Map;

import arrow.ArrowVectorHelper;

/**
 * Decode an object from an Arrow stream
 */
public class ARROWTO extends FormattedWarpScriptFunction {

  private final Arguments args;
  private static final String BYTES = "bytes";

  private final Arguments output;
  private static final String RESULT = "result";

  public Arguments getArguments() {
    return args;
  }
  public Arguments getOutput() {
    return output;
  }

  public ARROWTO(String name) {
    super(name);

    getDocstring().append("Decode a byte array that is in Arrow streaming format.");

    args = new ArgumentsBuilder()
      .addArgument(byte[].class, BYTES, "Arrow stream to be decoded." )
      .build();

    output = new ArgumentsBuilder()
      .addArgument(Object.class, RESULT, "The decoded object. GTS or ENCODER.")
      .build();

  }

  public WarpScriptStack apply(Map<String, Object> params, WarpScriptStack stack) throws WarpScriptException {

    byte[] in = (byte[]) params.get(BYTES);

    stack.push(ArrowVectorHelper.fromArrowStream(Channels.newChannel(new ByteArrayInputStream(in))));

    return stack;
  }
}
