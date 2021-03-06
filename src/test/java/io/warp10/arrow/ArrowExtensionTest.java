//
// Copyright 2020 SenX
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
import io.warp10.arrow.direct.ArrowHelper;
import io.warp10.arrow.pojo.ClassnameWarpField;
import io.warp10.arrow.pojo.ElevationWarpField;
import io.warp10.arrow.pojo.LatitudeWarpField;
import io.warp10.arrow.pojo.LongitudeWarpField;
import io.warp10.arrow.pojo.TimestampWarpField;
import io.warp10.arrow.pojo.ValueWarpField;
import io.warp10.arrow.warpscriptFunctions.ARROWTO;
import io.warp10.arrow.warpscriptFunctions.TOARROW;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.ASENCODERS;
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

  private static double ERROR = 1e-4;
  private static boolean doubleEquality(double a, double b) {
    return Math.abs(a - b) < ERROR;
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    StringBuilder props = new StringBuilder();

    props.append("warp.timeunits=us");
    WarpConfig.safeSetProperties(new StringReader(props.toString()));
    WarpScriptLib.register(new ArrowExtension());
  }

  private final static Random rng = new SecureRandom();

  @Test
  public void testNoError() throws Exception {

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();

    stack.exec("[ @senx/dataset/temperature bucketizer.mean 0 1 w 0 ] BUCKETIZE ->ARROW");
    stack.exec("'in' STORE { 'bytes' $in 'WarpScriptConversionMode' 'PAIR' }");
    //System.out.println(stack.dump(100));
    stack.drop();
  }

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

    stack.exec("'in' STORE { 'bytes' $in 'WarpScriptConversionMode' 'PAIR' }");
    stack.exec(ArrowExtension.ARROWTO);

    List out = (List) stack.pop();
    Map<String, List> res = (Map) out.get(1);
    stack.push(out.get(0));
    stack.exec("TYPEOF 'MAP' == ASSERT");

    stack.push(res.get(ArrowHelper.TIMESTAMPS_KEY));
    stack.push(res.get(ArrowHelper.LATITUDE_KEY));
    stack.push(res.get(ArrowHelper.LONGITUDE_KEY));
    stack.push(res.get(ArrowHelper.ELEVATION_KEY));
    stack.push(res.get(ArrowHelper.LONG_VALUES_KEY));
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

    int size = 10;
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
    stack.exec("'in' STORE { 'bytes' $in 'WarpScriptConversionMode' 'PAIR' }");
    stack.exec(ArrowExtension.ARROWTO);
    List res = (List) stack.pop();
    Map<String, String> metadata =  (Map<String, String>) res.get(0);

    stack.push(metadata);
    stack.exec("SIZE 3 ==");

    Map<String, List> cols = (Map<String, List>) res.get(1);

    Assert.isTrue(cols.containsKey(ClassnameWarpField.CLASSNAME_KEY));
    Assert.isTrue(cols.containsKey("type"));
    Assert.isTrue(cols.containsKey("id"));
    Assert.isTrue(cols.containsKey(LatitudeWarpField.LATITUDE_KEY));
    Assert.isTrue(cols.containsKey(LongitudeWarpField.LONGITUDE_KEY));
    Assert.isTrue(cols.containsKey(ElevationWarpField.ELEVATION_KEY));
    Assert.isTrue(cols.containsKey(ValueWarpField.LONG_VALUES_KEY));

    int count = 0;
    for (int j = 0; j < N; j++) {
      for (int i = 0; i < size; i++) {
        long tick = GTSHelper.tickAtIndex(list.get(j), i);
        double[] location = GeoXPLib.fromGeoXPPoint(GTSHelper.locationAtIndex(list.get(j), i));
        long elevation = GTSHelper.elevationAtIndex(list.get(j), i);
        long val = (Long) GTSHelper.valueAtIndex(list.get(j), i);

        Assert.isTrue(cols.get(ClassnameWarpField.CLASSNAME_KEY).get(count).equals("longGTS"));
        Assert.isTrue(cols.get("type").get(count).equals("LONG"));
        Assert.isTrue(cols.get("id").get(count).equals(String.valueOf(j)));

        Assert.isTrue(tick == (Long) cols.get(TimestampWarpField.TIMESTAMPS_KEY).get(count));
        Assert.isTrue(doubleEquality((Double) cols.get(LatitudeWarpField.LATITUDE_KEY).get(count), location[0]));
        Assert.isTrue(doubleEquality((Double) cols.get(LongitudeWarpField.LONGITUDE_KEY).get(count), location[1]));
        Assert.isTrue(elevation == (Long) cols.get(ElevationWarpField.ELEVATION_KEY).get(count));
        Assert.isTrue(val == (Long) cols.get(ValueWarpField.LONG_VALUES_KEY).get(count));
        count++;
      }
    }
  }

  @Test
  public void warpscriptTest_0() throws Exception {

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();

    //
    // Test that this warpscript exec without errors
    //

    String script = "NEWGTS 'a' RENAME { 'k' 'v' 'kk' 'vv' } RELABEL\n" +
      "0 NaN NaN NaN 1.0 ADDVALUE\n" +
      "4 NaN NaN NaN 1.0 ADDVALUE\n" +
      "'a' STORE\n" +
      "\n" +
      "NEWGTS 'b' RENAME { 'k' 'b' 'kk' 'vb' 'u' 'nn' } RELABEL\n" +
      "1 NaN NaN NaN 1.1 ADDVALUE\n" +
      "'b' STORE\n" +
      "\n" +
      "$a ->ARROW ARROW->\n" +
      "[ $a ] ->ARROW 'in' STORE { 'bytes' $in 'WarpScriptConversionMode' 'PAIR' } ARROW->\n" +
      "[ $a $b ] ->ARROW";

    stack.execMulti(script);
    stack.exec("'in' STORE { 'bytes' $in 'WarpScriptConversionMode' 'PAIR' }");

    // isolate bug for better stack trace
    WarpScriptStackFunction from = new ARROWTO("from");
    from.apply(stack);
  }

  @Test
  public void warpscriptTest_1() throws Exception {

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();

    //
    // Test that this warpscript exec without errors
    //

    String script = "NEWGTS 'a' RENAME { 'k' 'v' 'kk' 'vv' } RELABEL\n" +
      "'a' STORE\n" +
      "\n" +
      "NEWGTS 'b' RENAME { 'k' 'b' 'kk' 'vb' 'u' 'nn' } RELABEL\n" +
      "100 NaN NaN NaN 5.0 ADDVALUE\n" +
      "'b' STORE\n" +
      "\n" +
      "NEWGTS 'c' RENAME { 'k' 'b' 'kk' 'vb' 'u' 'ee' } RELABEL\n" +
      "100 RAND RAND NaN 5.0 ADDVALUE\n" +
      "100 RAND RAND NaN 5.0 ADDVALUE\n" +
      "'c' STORE\n" +
      "\n" +
      "NEWGTS 'd' RENAME { 'k' 'b' 'kk' 'vb' 'u' 'nn' } RELABEL\n" +
      "100 NaN NaN 42 5.0 ADDVALUE\n" +
      "'d' STORE\n" +
      "\n" +
      "NEWGTS 'e' RENAME { 'k' 'r' 'u' 'nn' 'm' 'mm' } RELABEL\n" +
      "100 RAND RAND 42 5.0 ADDVALUE\n" +
      "300 RAND RAND 4242 5.3 ADDVALUE\n" +
      "'e' STORE\n";
    stack.execMulti(script);

    WarpScriptStackFunction to = new TOARROW("to");
    WarpScriptStackFunction from = new ARROWTO("from");

    List<Object> list = new ArrayList<Object>();
    list.add(stack.load("a"));
    stack.push(list);
    to.apply(stack);
    stack.exec("'in' STORE { 'bytes' $in 'WarpScriptConversionMode' 'PAIR' }");
    from.apply(stack);

    list.add(stack.load("b"));
    stack.push(list);
    to.apply(stack);
    stack.exec("'in' STORE { 'bytes' $in 'WarpScriptConversionMode' 'PAIR' }");
    from.apply(stack);

    list.add(stack.load("c"));
    stack.push(list);
    to.apply(stack);
    stack.exec("'in' STORE { 'bytes' $in 'WarpScriptConversionMode' 'PAIR' }");
    from.apply(stack);

    list.add(stack.load("d"));
    stack.push(list);
    to.apply(stack);
    stack.exec("'in' STORE { 'bytes' $in 'WarpScriptConversionMode' 'PAIR' }");
    from.apply(stack);

    list.add(stack.load("e"));
    stack.push(list);
    to.apply(stack);
    stack.exec("'in' STORE { 'bytes' $in 'WarpScriptConversionMode' 'PAIR' }");
    from.apply(stack);

    WarpScriptStackFunction asEnc = new ASENCODERS("asEnc");
    stack.push(list);
    asEnc.apply(stack);
    to.apply(stack);
    stack.exec("'in' STORE { 'bytes' $in 'WarpScriptConversionMode' 'PAIR' }");
    from.apply(stack);

    System.out.println(stack.dump(100));
  }

  @Test
  public void warpscriptTest_2() throws Exception {

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();

    //
    // Test that this warpscript exec without errors
    //

    String script = "[ @senx/dataset/temperature 0 2 SUBLIST\n" +
      "bucketizer.mean 1508968800000000 1 h 4 ] BUCKETIZE\n" +
      "'data' STORE\n" +
      "[ $data 0 GET\n" +
      "  $data 1 GET -3 SHRINK\n" +
      "  $data 2 GET 0 SHRINK ]";

    stack.execMulti(script);

    WarpScriptStackFunction to = new TOARROW("to");
    WarpScriptStackFunction from = new ARROWTO("from");

    to.apply(stack);
    stack.exec("'in' STORE { 'bytes' $in 'WarpScriptConversionMode' 'PAIR' }");
    from.apply(stack);

    System.out.println(stack.dump(100));
  }
}
