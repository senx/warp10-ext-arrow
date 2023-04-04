//
// Copyright 2020-23 SenX
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

package io.warp10.arrow.warpscriptFunctions;

import io.warp10.arrow.convert.Register;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.ext.formatted.FormattedWarpScriptFunction;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Decode an Arrow byte array representation
 */
public class ARROWTO extends FormattedWarpScriptFunction {

  private final Arguments args;
  public static final String BYTES = "bytes";
  public static final String MODE = "WarpScriptConversionMode";

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

    getDocstring().append("Decode an Arrow stream (BYTES). The type of the result depends on the value of *WarpScriptConversionMode* (see the table in the extension's README). If the input's metadata has no value for *WarpScriptConversionMode*, the PAIR conversion mode will be used. \n" +
      "If the input has no value for the metadata *WarpScriptType*, it will use the default value PAIR.");

    args = new ArgumentsBuilder()
      .addArgument(byte[].class, BYTES, "Arrow stream to be decoded." )
      .addOptionalArgument(String.class, MODE, "WarpScriptConversionMode to use. If set, this value takes precedence for the choice of the conversion mode.", "")
      .build();

    output = new ArgumentsBuilder()
      .addArgument(Object.class, RESULT, "The decoded object.")
      .build();

  }

  public WarpScriptStack apply(Map<String, Object> params, WarpScriptStack stack) throws WarpScriptException {

    InputStream in = new ByteArrayInputStream((byte[]) params.get(BYTES));

    Object res = null;

    try (ArrowStreamReader reader = new ArrowStreamReader(in, new RootAllocator(Integer.MAX_VALUE))) {

      Map<String, String> metadata = reader.getVectorSchemaRoot().getSchema().getCustomMetadata();

      String conversionMode = Register.PAIR; // PAIR is default mode
      if ("" != params.get(MODE)) {
        conversionMode = (String) params.get(MODE);

      } else {
        String inputMode = metadata.get(MODE);

        if (null != inputMode) {
          conversionMode = inputMode;
        }
      }

      if (!Register.isSupportedMode(conversionMode)) {
        throw new WarpScriptException("WarpScriptConversionMode " + conversionMode + " is not supported.");
      }

      res = Register.getConverter(conversionMode).read(reader);

    } catch (IOException ioe) {
      throw new WarpScriptException(ioe);
    }

    stack.push(res);

    return stack;
  }
}
