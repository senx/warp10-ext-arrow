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

package io.warp10.arrow.direct;

import com.geoxp.GeoXPLib;
import io.warp10.Revision;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.WarpScriptException;
import io.warp10.script.functions.TYPEOF;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.boon.json.JsonSerializer;
import org.boon.json.JsonSerializerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.warp10.arrow.direct.ArrowHelper.BOOLEAN_VALUES_FIELD;
import static io.warp10.arrow.direct.ArrowHelper.BOOLEAN_VALUES_KEY;
import static io.warp10.arrow.direct.ArrowHelper.BUCKETCOUNT;
import static io.warp10.arrow.direct.ArrowHelper.BUCKETSPAN;
import static io.warp10.arrow.direct.ArrowHelper.BYTES_VALUES_FIELD;
import static io.warp10.arrow.direct.ArrowHelper.BYTES_VALUES_KEY;
import static io.warp10.arrow.direct.ArrowHelper.DOUBLE_VALUES_FIELD;
import static io.warp10.arrow.direct.ArrowHelper.DOUBLE_VALUES_KEY;
import static io.warp10.arrow.direct.ArrowHelper.ELEVATION_FIELD;
import static io.warp10.arrow.direct.ArrowHelper.ELEVATION_KEY;
import static io.warp10.arrow.direct.ArrowHelper.LASTBUCKET;
import static io.warp10.arrow.direct.ArrowHelper.LATITUDE_FIELD;
import static io.warp10.arrow.direct.ArrowHelper.LATITUDE_KEY;
import static io.warp10.arrow.direct.ArrowHelper.LONGITUDE_FIELD;
import static io.warp10.arrow.direct.ArrowHelper.LONGITUDE_KEY;
import static io.warp10.arrow.direct.ArrowHelper.LONG_VALUES_FIELD;
import static io.warp10.arrow.direct.ArrowHelper.LONG_VALUES_KEY;
import static io.warp10.arrow.direct.ArrowHelper.REV;
import static io.warp10.arrow.direct.ArrowHelper.STRING_VALUES_FIELD;
import static io.warp10.arrow.direct.ArrowHelper.STRING_VALUES_KEY;
import static io.warp10.arrow.direct.ArrowHelper.STU;
import static io.warp10.arrow.direct.ArrowHelper.TIMESTAMPS_KEY;
import static io.warp10.arrow.direct.ArrowHelper.TIMESTAMP_FIELD;
import static io.warp10.arrow.direct.ArrowHelper.TYPE;

public class ArrowWriters {

  //
  // Json converters for labels and attributes
  //

  final static JsonSerializer serializer = new JsonSerializerFactory().create();
  private static String toJson(Object o) {
    return serializer.serialize(o).toString();
  }

  /**
   * Extract Gts Metadata into a Map that can be used as metadata of an Arrow Schema
   *
   * @param gtsMeta
   * @return
   */
  public static Map<String, String> extractGtsMetadata(Metadata gtsMeta) {

    Map<String, String> metadata = new HashMap<>();

    if (gtsMeta.isSetName()) {
      metadata.put(Metadata._Fields.NAME.getFieldName(), gtsMeta.getName());
    }

    if (gtsMeta.isSetLabels() && gtsMeta.getLabels().size() > 0) {
      metadata.put(Metadata._Fields.LABELS.getFieldName(), toJson(gtsMeta.getLabels()));
    }

    if(gtsMeta.isSetClassId()) {
      metadata.put(Metadata._Fields.CLASS_ID.getFieldName(), String.valueOf(gtsMeta.getClassId()));
    }

    if(gtsMeta.isSetLabelsId()) {
      metadata.put(Metadata._Fields.LABELS_ID.getFieldName(), String.valueOf(gtsMeta.getLabelsId()));
    }

    if (gtsMeta.isSetAttributes() && gtsMeta.getAttributes().size() > 0) {
      metadata.put(Metadata._Fields.ATTRIBUTES.getFieldName(), toJson(gtsMeta.getAttributes()));
    }

    if (gtsMeta.isSetSource()) {
      metadata.put(Metadata._Fields.SOURCE.getFieldName(), gtsMeta.getSource());
    }

    if(gtsMeta.isSetLastActivity()) {
      metadata.put(Metadata._Fields.LAST_ACTIVITY.getFieldName(), String.valueOf(gtsMeta.getLastActivity()));
    }

    return metadata;
  }

  //
  // GTS to Arrow
  //

  /**
   * Creates an Arrow schema fitted to a GTS
   * @param gts
   * @return
   */
  public static Schema createGtsSchema(GeoTimeSerie gts) throws WarpScriptException {

    List<Field> fields = new ArrayList<>();

    //
    // Feed schema's metadata
    //

    Map<String, String> metadata = extractGtsMetadata(gts.getMetadata());
    metadata.put(TYPE, TYPEOF.typeof(gts));
    metadata.put(REV, Revision.REVISION);
    metadata.put(STU, String.valueOf(Constants.TIME_UNITS_PER_S));

    //
    // Bucketize info
    //

    if (GTSHelper.isBucketized(gts)) {
      metadata.put(BUCKETSPAN, String.valueOf(GTSHelper.getBucketSpan(gts)));
      metadata.put(BUCKETCOUNT, String.valueOf(GTSHelper.getBucketCount(gts)));
      metadata.put(LASTBUCKET, String.valueOf(GTSHelper.getLastBucket(gts)));
    }

    //
    // Feed schema's fields
    //

    if (0 == gts.size()) {
      return new Schema(fields, metadata);
    }

    fields.add(TIMESTAMP_FIELD);

    if (gts.hasLocations()) {
      fields.add(LATITUDE_FIELD);
      fields.add(LONGITUDE_FIELD);
    }

    if (gts.hasElevations()) {
      fields.add(ELEVATION_FIELD);
    }

    GeoTimeSerie.TYPE type = gts.getType();
    switch(type) {
      case LONG: fields.add(LONG_VALUES_FIELD);
        break;

      case DOUBLE: fields.add(DOUBLE_VALUES_FIELD);
        break;

      case BOOLEAN: fields.add(BOOLEAN_VALUES_FIELD);
        break;

      case STRING: fields.add(STRING_VALUES_FIELD);
        break;

      case UNDEFINED: throw new WarpScriptException("Cannot create an Arrow schema for a GTS with data of undefined type.");
    }

    return new Schema(fields, metadata);
  }

  /**
   * Convert a GTS to an output stream in arrow format
   */

  public static void gtsToArrowStream(GeoTimeSerie gts, OutputStream out) throws WarpScriptException {
    gtsToArrowStream(gts, gts.size(), out);
  }

  public static void gtsToArrowStream(GeoTimeSerie gts, int nTicksPerBatch, OutputStream out) throws WarpScriptException {

    VectorSchemaRoot root = VectorSchemaRoot.create(createGtsSchema(gts), new RootAllocator(Integer.MAX_VALUE));

    if (gts.size() == 0) {
      return;
    }

    //
    // Feed data to root
    //

    try (ArrowStreamWriter writer =  new ArrowStreamWriter(root, null, out)) {

      writer.start();
      root.setRowCount(nTicksPerBatch);

      for (int i = 0; i < gts.size(); i++) {

        ((BigIntVector) root.getVector(TIMESTAMPS_KEY)).setSafe(i % nTicksPerBatch, GTSHelper.tickAtIndex(gts, i));

        if (gts.hasLocations()) {
          double[] latlon = GeoXPLib.fromGeoXPPoint(GTSHelper.locationAtIndex(gts, i));
          ((Float4Vector) root.getVector(LATITUDE_KEY)).setSafe(i % nTicksPerBatch, (float) latlon[0]);
          ((Float4Vector) root.getVector(LONGITUDE_KEY)).setSafe(i % nTicksPerBatch, (float) latlon[1]);
        }

        if (gts.hasElevations()) {
          ((BigIntVector) root.getVector(ELEVATION_KEY)).setSafe(i % nTicksPerBatch, GTSHelper.elevationAtIndex(gts, i));
        }

        GeoTimeSerie.TYPE type = gts.getType();
        switch(type) {
          case LONG: ((BigIntVector) root.getVector(LONG_VALUES_KEY)).setSafe(i % nTicksPerBatch, (long) GTSHelper.valueAtIndex(gts, i));
            break;

          case DOUBLE: ((Float8Vector) root.getVector(DOUBLE_VALUES_KEY)).setSafe(i % nTicksPerBatch, (double) GTSHelper.valueAtIndex(gts, i));
            break;

          case BOOLEAN: ((BitVector) root.getVector(BOOLEAN_VALUES_KEY)).setSafe(i % nTicksPerBatch, (boolean) GTSHelper.valueAtIndex(gts, i) ? 1 : 0);
            break;

          case STRING: ((VarBinaryVector) root.getVector(STRING_VALUES_KEY)).setSafe(i % nTicksPerBatch, ((String) GTSHelper.valueAtIndex(gts, i)).getBytes());
            break;

          case UNDEFINED: throw new WarpScriptException("Cannot create an Arrow stream for a GTS with data of undefined type.");
        }

        if (i % nTicksPerBatch == nTicksPerBatch - 1) {
          writer.writeBatch();
        }
      }

      if ((gts.size() - 1) % nTicksPerBatch != nTicksPerBatch - 1 ) {
        root.setRowCount((gts.size() - 1) % nTicksPerBatch);
        writer.writeBatch();
      }

      writer.end();
    } catch (IOException e) {
      throw new WarpScriptException(e);
    } finally {
      root.close();
    }
  }

  //
  // GtsEncoder to Arrow
  //


  /**
   * Creates an Arrow schema fitted to a GtsEncoder
   * @param encoder
   * @return
   */
  public static Schema createGtsEncoderSchema(GTSEncoder encoder) throws WarpScriptException {

    List<Field> fields = new ArrayList<>();

    //
    // Feed schema's metadata
    //

    Map<String, String> metadata;

    Metadata meta = encoder.getRawMetadata();

    if (null != meta) {
      metadata = extractGtsMetadata(meta);

    } else {
      metadata = new HashMap<String, String>();
    }

    metadata.put(TYPE, TYPEOF.typeof(encoder));
    metadata.put(REV, Revision.REVISION);
    metadata.put(STU, String.valueOf(Constants.TIME_UNITS_PER_S));

    //
    // Feed schema's fields
    //

    if (0 == encoder.size()) {
      return new Schema(fields, metadata);
    }

    fields.add(TIMESTAMP_FIELD);
    fields.add(LATITUDE_FIELD);
    fields.add(LONGITUDE_FIELD);
    fields.add(ELEVATION_FIELD);
    fields.add(LONG_VALUES_FIELD);
    fields.add(DOUBLE_VALUES_FIELD);
    //fields.add(BIGDECIMAL_VALUES_FIELD);
    //fields.add(BIGDECIMAL_SCALES_FIELD);
    fields.add(BOOLEAN_VALUES_FIELD);
    fields.add(STRING_VALUES_FIELD);
    fields.add(BYTES_VALUES_FIELD);

    return new Schema(fields, metadata);
  }

  /**
   * Convert a GtsEncoder to an arrow stream
   */
  public static void gtsEncodertoArrowStream(GTSEncoder encoder, int nTicksPerBatch, OutputStream out) throws WarpScriptException {

    VectorSchemaRoot root = VectorSchemaRoot.create(createGtsEncoderSchema(encoder), new RootAllocator(Integer.MAX_VALUE));

    //
    // Feed data to root
    //

    try (ArrowStreamWriter writer =  new ArrowStreamWriter(root, null, out)) {

      writer.start();
      root.setRowCount(nTicksPerBatch);

      GTSDecoder decoder = encoder.getDecoder(true);

      int i = 0;
      while (decoder.next()) {

        // tick
        ((BigIntVector) root.getVector(TIMESTAMPS_KEY)).setSafe(i % nTicksPerBatch, decoder.getTimestamp());

        // location
        double[] latlon = GeoXPLib.fromGeoXPPoint(decoder.getLocation());
        ((Float4Vector) root.getVector(LATITUDE_KEY)).setSafe(i % nTicksPerBatch, (float) latlon[0]);
        ((Float4Vector) root.getVector(LONGITUDE_KEY)).setSafe(i % nTicksPerBatch, (float) latlon[1]);

        // elevation
        ((BigIntVector) root.getVector(ELEVATION_KEY)).setSafe(i % nTicksPerBatch, decoder.getElevation());

        //
        // value:
        // long, boolean, double, BigDecimal, String, StringBinary
        //

        Object value = decoder.getBinaryValue();

        if (value instanceof Long) {
          ((BigIntVector) root.getVector(LONG_VALUES_KEY)).setSafe(i % nTicksPerBatch, (long) value);

        } else if (value instanceof Boolean) {
          ((BitVector) root.getVector(BOOLEAN_VALUES_KEY)).setSafe(i % nTicksPerBatch, (boolean) value ? 1 : 0);

        } else if (value instanceof Double) {
          ((Float8Vector) root.getVector(DOUBLE_VALUES_KEY)).setSafe(i % nTicksPerBatch, (double) value);

        } else if (value instanceof BigDecimal) {
          ((Float8Vector) root.getVector(DOUBLE_VALUES_KEY)).setSafe(i % nTicksPerBatch, ((BigDecimal) value).doubleValue());
          //((BigIntVector) root.getVector(BIGDECIMAL_VALUES_KEY)).setSafe(i % nTicksPerBatch, ((BigDecimal) value).longValue());
          //((BigIntVector) root.getVector(BIGDECIMAL_SCALES_KEY)).setSafe(i % nTicksPerBatch, ((BigDecimal) value).scale());

        } else if (value instanceof String) {
          ((VarCharVector) root.getVector(STRING_VALUES_KEY)).setSafe(i % nTicksPerBatch, new Text((String) value));

        } else if (value instanceof byte[]) {
          ((VarBinaryVector) root.getVector(BYTES_VALUES_KEY)).setSafe(i % nTicksPerBatch, ((String) value).getBytes());

        } else {
          throw new WarpScriptException("Unrecognized value type when trying to convert a GTSENCODER to an Arrow Stream");
        }

        if (i % nTicksPerBatch == nTicksPerBatch - 1) {
          writer.writeBatch();
        }

        i++;
      }

      if ((encoder.getCount() - 1) % nTicksPerBatch != nTicksPerBatch - 1 ) {
        root.setRowCount(((int) encoder.getCount() - 1) % nTicksPerBatch);
        writer.writeBatch();
      }

      writer.end();
    } catch (IOException e) {
      throw new WarpScriptException(e);
    } finally {
      root.close();
    }
  }

  /**
   * Convert a map of columns (Lists) into an Arrow Stream.
   * The columns must have the same size.
   * @param input A list of two items: custom metadata and map of columns (assumed to be of same size > 0)
   * @param out
   * @throws WarpScriptException
   */
  public static void columnsToArrowStream(List input, int nTicksPerBatch, OutputStream out) throws WarpScriptException {

    Map<String, String> customMetadata = new HashMap<String, String>((Map<String, String>) input.get(0));
    customMetadata.put(TYPE, TYPEOF.typeof(input));
    customMetadata.put(REV, Revision.REVISION);
    customMetadata.put(STU, String.valueOf(Constants.TIME_UNITS_PER_S));

    Map<String, List> columns = (Map<String, List>) input.get(1);
    List<Field> fields = new ArrayList<Field>(columns.size());
    for (String key: columns.keySet()) {
      Object first = columns.get(key).get(0);

      if (first instanceof Boolean) {
        fields.add(Field.nullable(key, new ArrowType.Bool()));

      } else if (first instanceof Long) {
        fields.add(Field.nullable(key,new ArrowType.Int(64, true)));

      } else if (first instanceof Double) {
        fields.add(Field.nullable(key, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));

      } else if (first instanceof String) {
        fields.add(Field.nullable(key, new ArrowType.Utf8()));

      } else if (first instanceof byte[]) {
        fields.add(Field.nullable(key, new ArrowType.Binary()));

      } else {
        throw new WarpScriptException("Unsupported field vector type. Support BOOLEAN, LONG, DOUBLE, STRING or BYTES.");
      }
    }

    VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields, customMetadata), new RootAllocator(Integer.MAX_VALUE));

    //
    // Feed data to root
    //

    try (ArrowStreamWriter writer =  new ArrowStreamWriter(root, null, out)) {

      writer.start();
      root.setRowCount(nTicksPerBatch);

      int count = columns.get(columns.keySet().iterator().next()).size();
      for (int i = 0; i < count; i++) {

        for (Field field : root.getSchema().getFields()) {

          Object value = columns.get(field.getName()).get(i);

          if (value instanceof Boolean) {
            ((BitVector) root.getVector(field.getName())).setSafe(i % nTicksPerBatch, (boolean) value ? 1 : 0);

          } else if (value instanceof Long) {
            ((BigIntVector) root.getVector(field.getName())).setSafe(i % nTicksPerBatch, (long) value);

          } else if (value instanceof Double) {
            ((Float8Vector) root.getVector(field.getName())).setSafe(i % nTicksPerBatch, (double) value);

          } else if (value instanceof String) {
            ((VarCharVector) root.getVector(field.getName())).setSafe(i % nTicksPerBatch, new Text((String) value));

          } else if (value instanceof byte[]) {
            ((VarBinaryVector) root.getVector(field.getName())).setSafe(i % nTicksPerBatch, ((String) value).getBytes());

          } else {
            throw new WarpScriptException("Unsupported field vector type. Support BOOLEAN, LONG, DOUBLE, STRING or BYTES.");
          }
        }

        if (i % nTicksPerBatch == nTicksPerBatch - 1) {
          writer.writeBatch();
        }
      }

      if ((count - 1) % nTicksPerBatch != nTicksPerBatch - 1 ) {
        root.setRowCount((count - 1) % nTicksPerBatch);
        writer.writeBatch();
      }

      writer.end();
    } catch (IOException e) {
      throw new WarpScriptException(e);
    } finally {
      root.close();
    }
  }
}
