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

import io.warp10.arrow.convert.Register;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.formatted.FormattedWarpScriptFunction;

import java.io.ByteArrayOutputStream;
import java.util.Map;

/**
 * Encode an Object as an Arrow stream
 */
public class TOARROW extends FormattedWarpScriptFunction {

  public static final String IN = "in";
  public static final String OUT = "out";

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
    ByteArrayOutputStream out =  new ByteArrayOutputStream();
    boolean found = false;
    for (String type: Register.getKnownConversionModes()) {
      if (Register.getConverter(type).isConvertible(in)) {
        Register.getConverter(type).write(in, out);
        found = true;
        break;
      }
    }

    if (!found) {
      throw new WarpScriptException("Input is not convertible to Arrow columnar format.");
    }

    stack.push(out.toByteArray());

    return stack;
  }
}
