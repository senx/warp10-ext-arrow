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

package arrow;

import com.geoxp.GeoXPLib;
import com.vividsolutions.jts.util.Assert;
import io.warp10.WarpConfig;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptLib;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.security.SecureRandom;
import java.util.Random;

public class ArrowExtensionTest {

  private static double ERROR = 1e-5;
  private static boolean doubleEquality(double a, double b) {
    return Math.abs(a - b) < ERROR;
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    StringBuilder props = new StringBuilder();

    props.append("warp.timeunits=us");
    WarpConfig.setProperties(new StringReader(props.toString()));
    WarpScriptLib.register(new ArrowExtension());
  }

  private final static Random rng = new SecureRandom();

  @Test
  public void roundTripLongGTS() throws Exception {

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();

    int size = 100000;
    GeoTimeSerie gts = new GeoTimeSerie(size);

    long[] ticks = new long[size];
    long[] location = new long[size];
    long[] elevation = new long[size];
    long[] longs = new long[size];

    for (int i = 0; i < size; i++) {
      ticks[i] = i * Constants.TIME_UNITS_PER_S;
      location[i] = rng.nextLong();
      elevation[i] = rng.nextLong();
      longs[i] = rng.nextLong();
    }

    gts.reset(ticks, location, elevation, longs, size);

    gts.setName("doubleGTS");
    stack.push(gts);
    stack.exec("{ 'type' 'DOUBLE' } RELABEL");

    stack.exec(ArrowExtension.TOARROW);
    stack.exec(ArrowExtension.ARROWTO);

    GeoTimeSerie gts_res = (GeoTimeSerie) stack.get(0);
    stack.exec("TYPEOF 'GTS' == ASSERT");

    Assert.equals(gts.size(), gts_res.size());
    Assert.isTrue(gts.getMetadata().equals(gts_res.getMetadata()));

    for (int i = 0; i < gts.size(); i++) {

      Assert.equals(GTSHelper.tickAtIndex(gts,i), GTSHelper.tickAtIndex(gts_res, i));
      double[] latlon = GeoXPLib.fromGeoXPPoint(GTSHelper.locationAtIndex(gts, i));
      double[] latlon_res = GeoXPLib.fromGeoXPPoint(GTSHelper.locationAtIndex(gts_res, i));
      Assert.isTrue(doubleEquality(latlon[0], latlon_res[0]));
      Assert.isTrue(doubleEquality(latlon[1], latlon_res[1]));
      Assert.equals(GTSHelper.elevationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts_res, i));
      Assert.equals(GTSHelper.valueAtIndex(gts, i), GTSHelper.valueAtIndex(gts_res, i));
    }

  }
}
