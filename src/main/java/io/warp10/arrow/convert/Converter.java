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

package io.warp10.arrow.convert;

import io.warp10.script.WarpScriptException;

import java.io.InputStream;
import java.io.OutputStream;

public interface Converter<T> {

    String getWarpScriptConversionMode();

    boolean isConvertible(Object o);

    void write(T object, OutputStream out) throws WarpScriptException;

    T read(InputStream in) throws WarpScriptException;

    /**
     * Used if need to choose between converters of same conversion mode.
     * @return the level
     */
    default int getPriorityLevel(){
        return 0;
    }
}
