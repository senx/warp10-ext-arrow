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

package io.warp10.arrow;

import com.geoxp.GeoXPLib;
import com.vividsolutions.jts.util.Assert;
import io.warp10.WarpConfig;
import io.warp10.arrow.direct.ArrowVectorHelper;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptLib;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    gts.setName("longGTS");
    stack.push(gts);
    stack.exec("{ 'type' 'LONG' } RELABEL");

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

  @Test
  public void roundTripEncoderViaLongGTS() throws Exception {

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

    gts.setName("longGTS");
    stack.push(gts);
    stack.exec("{ 'type' 'LONG' } RELABEL");

    stack.exec(WarpScriptLib.ASENCODERS);
    stack.exec(ArrowExtension.TOARROW);
    stack.exec(ArrowExtension.ARROWTO);
    stack.exec(WarpScriptLib.TOGTS);
    stack.exec("'LONG' GET");

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

  @Test
  public void roundTripDoubleDefaultOutputToDefaultOutput() throws Exception {

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();
    Random rand = new Random();

    int size = 3;
    int nCols = 2;

    Map<String, List<Double>> cols = new LinkedHashMap<String, List<Double>>(nCols);
    for (int i = 0; i < nCols; i++) {

      List<Double> col =  new ArrayList<Double>(size);
      for (int j = 0; j < size; j++) {

        col.add(rand.nextDouble());
      }

      cols.put(String.valueOf(i), col);
    }

    List<Map> pair = new ArrayList<>(2);
    pair.add(new LinkedHashMap<>());
    pair.add(cols);
    stack.push(pair);
    stack.exec(ArrowExtension.TOARROW);
    stack.exec(ArrowExtension.ARROWTO);

    List out = (List) stack.pop();
    Map<String, List<Double>> res = (Map) out.get(1);
    stack.push(out.get(0));
    stack.exec("DUP TYPEOF 'MAP' == ASSERT SIZE 3 == ASSERT");

    for (int i = 0; i < nCols; i++) {
      for (int j = 0; j < size; j++) {

        Assert.isTrue(doubleEquality(cols.get(String.valueOf(i)).get(j), res.get(String.valueOf(i)).get(j)));
      }
    }
  }

  @Test
  public void roundTripLongGTSDefaultOutput() throws Exception {

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

    gts.setName("longGTS");
    stack.push(gts);
    stack.exec("{ 'type' 'LONG' } RELABEL");

    stack.exec(ArrowExtension.TOARROW);

    stack.exec("'in' STORE { 'bytes' $in 'default' true }");
    stack.exec(ArrowExtension.ARROWTO);

    List out = (List) stack.pop();
    Map<String, List> res = (Map) out.get(1);
    stack.push(out.get(0));
    stack.exec("TYPEOF 'MAP' == ASSERT");

    stack.push(res.get(ArrowVectorHelper.TIMESTAMPS_KEY));
    stack.push(res.get(ArrowVectorHelper.LATITUDE_KEY));
    stack.push(res.get(ArrowVectorHelper.LONGITUDE_KEY));
    stack.push(res.get(ArrowVectorHelper.ELEVATION_KEY));
    stack.push(res.get(ArrowVectorHelper.LONG_VALUES_KEY));
    stack.exec(WarpScriptLib.MAKEGTS);

    GeoTimeSerie gts_res = (GeoTimeSerie) stack.pop();

    Assert.equals(gts.size(), gts_res.size());

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

  @Test
  public void GtsListToArrowToWarpScripMapOfLists() throws Exception {

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();

    int size = 10000;
    int N = 10;
    List<GeoTimeSerie> list = new ArrayList<>(N);

    for (int j = 0; j < N; j++) {

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

      gts.setName("longGTS");

      Map<String, String> labels = new HashMap<String, String>(2);
      labels.put("type", "LONG");
      labels.put("id", String.valueOf(j));
      gts.setLabels(labels);

      list.add(gts);
    }

    stack.push(list);
    stack.exec(ArrowExtension.TOARROW);
    stack.exec(ArrowExtension.ARROWTO);
    List res = (List) stack.pop();
    Map<String, String> metadata =  (Map<String, String>) res.get(0);

    System.out.println(metadata);
    System.out.println(res.get(1));

    
    //TODO


  }
}
