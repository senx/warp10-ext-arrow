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

import io.warp10.arrow.direct.ArrowWriters;
import io.warp10.arrow.pojo.WarpSchema;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.WarpScriptException;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Register {

    private final static Map<String, Converter> REGISTER = new HashMap<String, Converter>();

    public static void addConverter(Converter converter){
        if (!REGISTER.containsKey(converter.getWarpScriptConversionMode()) || converter.getPriorityLevel() <= REGISTER.get(converter.getWarpScriptConversionMode()).getPriorityLevel()) {
            REGISTER.put(converter.getWarpScriptConversionMode(), converter);
        }
    }

    public static Set<String> getKnownConversionModes() {
        return REGISTER.keySet();
    }

    public static Converter getConverter(String type) {
      return REGISTER.get(type);
    }

    public static boolean hasConverterForType(String type) {
        return REGISTER.containsKey(type);
    }

    //
    // Base converters
    //

    static {

        //
        // ENCODERS
        //

        addConverter(new Converter<List>() {
            @Override
            public String getWarpScriptConversionMode() {
                return "ENCODERS";
            }

            @Override
            public boolean isConvertible(Object o) {
                if (!(o instanceof List)) {
                    return false;
                }

                for (Object oo: (List) o) {
                    if (!(oo instanceof GeoTimeSerie) && !(oo instanceof GTSEncoder)) {
                        return false;
                    }
                }

                return true;
            }

            @Override
            public void write(List list, OutputStream out) throws WarpScriptException {
                WarpSchema.GtsOrEncoderListSchema(list).writeListToStream(out, list);
            }

            @Override
            public List read(InputStream in) {
                //TODO
                return null;
            }
        });

        //
        // GTS
        //

        addConverter(new Converter<GeoTimeSerie>() {
            @Override
            public String getWarpScriptConversionMode() {
                return "GTS";
            }

            @Override
            public boolean isConvertible(Object o) {
                return o instanceof GeoTimeSerie;
            }

            @Override
            public void write(GeoTimeSerie gts, OutputStream out) throws WarpScriptException {
                ArrowWriters.gtsToArrowStream(gts, out);
            }

            @Override
            public GeoTimeSerie read(InputStream in) throws WarpScriptException {
                //TODO
                return null;
            }
        });

        //
        // PAIR
        //

        addConverter(new Converter<List>() {
            @Override
            public String getWarpScriptConversionMode() {
                return "PAIR";
            }

            @Override
            public boolean isConvertible(Object o) {
                if (!(o instanceof List)) {
                    return false;

                }

                List list = (List) o;
                if (2 != list.size() || !(list.get(0) instanceof Map) || !(list.get(1) instanceof Map)) {
                    return false;
                }

                return true;
            }

            @Override
            public void write(List list, OutputStream out) throws WarpScriptException {

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
                            throw new WarpScriptException("Incoherent list sizes for PAIR conversion mode. They must be equal.");
                        }
                    }
                }

                ArrowWriters.columnsToArrowStream(list, commonSize, out);
            }

            @Override
            public List read(InputStream in) {
                //TODO
                return null;
            }
        });
    }
}
