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

package io.warp10.arrow;

import io.warp10.arrow.warpscriptFunctions.ARROWTO;
import io.warp10.arrow.warpscriptFunctions.TOARROW;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.warp.sdk.WarpScriptExtension;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.HashMap;
import java.util.Map;

public class ArrowExtension extends WarpScriptExtension {

  private static final Map<String, Object> functions;

  public static final String TOARROW = "->ARROW";
  public static final String ARROWTO = "ARROW->";

  /**
   * The allocator used to allocate arrow buffers
   */

  public static final BufferAllocator rootAllocator = new RootAllocator();
  public static  BufferAllocator getRootAllocator() {
    return rootAllocator;
  }

  /**
   * If needed, it's possible to configure one child allocator per stack with the method below
   */

  private static final String ATTRIBUTE_ARROW_ALLOCATOR = "stack.arrow.allocator";
  public static BufferAllocator getAllocator(WarpScriptStack stack) throws WarpScriptException {

    // TODO: define max alloc per stack
    long maxAlloc = Long.MAX_VALUE;

    if (null == stack.getAttribute(ATTRIBUTE_ARROW_ALLOCATOR)) {
      stack.setAttribute(ATTRIBUTE_ARROW_ALLOCATOR, rootAllocator.newChildAllocator("stackAllocator", 0, maxAlloc));
    }

    return (BufferAllocator) stack.getAttribute(ATTRIBUTE_ARROW_ALLOCATOR);
  }

  static {
    functions = new HashMap<String, Object>();

    addFunction(new TOARROW(TOARROW));
    addFunction(new ARROWTO(ARROWTO));
  }

  private static void addFunction(NamedWarpScriptFunction fun) {
    functions.put(fun.getName(), fun);
  }
  public Map<String, Object> getFunctions() { return functions; }
  public static Map<String, Object> staticGetFunctions() { return functions; }
}
