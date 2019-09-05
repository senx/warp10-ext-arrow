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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.boon.json.JsonParser;
import org.boon.json.JsonParserFactory;
import org.boon.json.JsonSerializer;
import org.boon.json.JsonSerializerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utilities and converters
 *
 * TODO(JC): maybe refactor into multiple classes (one reader/writer per subtype and type).
 */
public class ArrowAdapterHelper {

  final static String TIMESTAMPS_KEY = "timestamp";
  final static String LONG_VALUES_KEY = TYPEOF.typeof(Long.class);
  final static String DOUBLE_VALUES_KEY = TYPEOF.typeof(Double.class);
  final static String BIGDECIMAL_VALUES_KEY = "BIGDECIMAL.CONTENT";
  final static String BIGDECIMAL_SCALES_KEY = "BIGDECIMAL.SCALE";
  final static String BOOLEAN_VALUES_KEY = TYPEOF.typeof(Boolean.class);
  final static String STRING_VALUES_KEY = TYPEOF.typeof(String.class);
  final static String BYTES_VALUES_KEY = TYPEOF.typeof(byte[].class);
  final static String LATITUDE_KEY = "latitude";
  final static String LONGITUDE_KEY = "longitude";
  final static String ELEVATION_KEY = "elevation";

  final static String BUCKETSPAN = "bucketspan";
  final static String BUCKETCOUNT = "bucketcount";
  final static String LASTBUCKET = "lastbucket";

  final static String TYPE = "WarpScriptType";
  final static String REV = "WarpScriptVersion";
  final static String STU = "WarpScriptSecondsPerTimeUnit";

  //
  // Fields of arrow schemas
  // Except for timestamp field, we make them nullable so we can use them for GTSEncoders
  //

  private static Field nonNullable(String key, ArrowType type) {
    return new Field(key, new FieldType(false, type, null), null);
  }

  // GTS fields
  final static Field TIMESTAMP_FIELD = nonNullable(TIMESTAMPS_KEY, new ArrowType.Int(64, true));
  final static Field LONG_VALUES_FIELD = Field.nullable(LONG_VALUES_KEY,new ArrowType.Int(64, true));
  final static Field DOUBLE_VALUES_FIELD = Field.nullable(DOUBLE_VALUES_KEY, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
  final static Field BOOLEAN_VALUES_FIELD = Field.nullable(BOOLEAN_VALUES_KEY, new ArrowType.Bool());
  final static Field STRING_VALUES_FIELD = Field.nullable(STRING_VALUES_KEY, new ArrowType.Binary());
  final static Field LATITUDE_FIELD = Field.nullable(LATITUDE_KEY, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
  final static Field LONGITUDE_FIELD = Field.nullable(LONGITUDE_KEY, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
  final static Field ELEVATION_FIELD = Field.nullable(ELEVATION_KEY, new ArrowType.Int(64, true));

  // additional fields for GTSEncoders
  final static Field UTF8_VALUES_FIELD = Field.nullable(STRING_VALUES_KEY, new ArrowType.Utf8()); // replaces STRING_VALUES_FIELD
  final static Field BYTES_VALUES_FIELD = Field.nullable(BYTES_VALUES_KEY, new ArrowType.Binary());
  final static Field BIGDECIMAL_VALUES_FIELD = Field.nullable(BIGDECIMAL_VALUES_KEY, new ArrowType.Int(64, true));
  final static Field BIGDECIMAL_SCALES_FIELD = Field.nullable(BIGDECIMAL_SCALES_KEY, new ArrowType.Int(32, true));

  //
  // Json converters for labels and attributes
  //

  final static JsonSerializer serializer = new JsonSerializerFactory().create();
  private static String toJson(Object o) {
    return serializer.serialize(o).toString();
  }

  final static JsonParser parser = new JsonParserFactory().create();
  private static Object fromJson(String s) {
    return parser.parse(s);
  }

  /**
   * Extract Gts Metadata into a Map that can be used as metadata of an Arrow Schema
   *
   * @param gtsMeta
   * @return
   */
  private static Map<String, String> extractGtsMetadata(Metadata gtsMeta) {

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

  /**
   * Retrieve Gts Metadata from an Arrow Schema
   * @param schema
   * @return
   */
  private static Metadata retrieveGtsMetadata(Schema schema) {

    Metadata gtsMeta = new Metadata();
    Map<String, String> metadata = schema.getCustomMetadata();

    gtsMeta.setName(metadata.get(Metadata._Fields.NAME.getFieldName()));
    gtsMeta.setLabels((Map<String, String>) fromJson(metadata.get(Metadata._Fields.LABELS.getFieldName())));
    gtsMeta.setClassId(Long.valueOf(metadata.get(Metadata._Fields.CLASS_ID.getFieldName())).longValue());
    gtsMeta.setLabelsId(Long.valueOf(metadata.get(Metadata._Fields.LABELS_ID.getFieldName())).longValue());
    gtsMeta.setAttributes((Map<String, String>) fromJson(metadata.get(Metadata._Fields.ATTRIBUTES.getFieldName())));
    gtsMeta.setSource(metadata.get(Metadata._Fields.SOURCE.getFieldName()));
    gtsMeta.setLastActivity(Long.valueOf(metadata.get(Metadata._Fields.LAST_ACTIVITY.getFieldName())).longValue());

    return gtsMeta;
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
  public static void gtstoArrowStream(GeoTimeSerie gts, int nTicksPerBatch, OutputStream out) throws WarpScriptException {

    VectorSchemaRoot root = VectorSchemaRoot.create(createGtsSchema(gts), new RootAllocator(Integer.MAX_VALUE));

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
    fields.add(BIGDECIMAL_VALUES_FIELD);
    fields.add(BIGDECIMAL_SCALES_FIELD);
    fields.add(BOOLEAN_VALUES_FIELD);
    fields.add(UTF8_VALUES_FIELD);
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
        ((BigIntVector) root.getVector(TIMESTAMPS_KEY)).setSafe(i % nTicksPerBatch, decoder.getBaseTimestamp());

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
          ((BigIntVector) root.getVector(LONG_VALUES_KEY)).setSafe(i % nTicksPerBatch, (long) decoder.getValue());

        } else if (value instanceof Boolean) {
          ((BitVector) root.getVector(BOOLEAN_VALUES_KEY)).setSafe(i % nTicksPerBatch, (boolean) decoder.getValue() ? 1 : 0);

        } else if (value instanceof Double) {
          ((Float8Vector) root.getVector(DOUBLE_VALUES_KEY)).setSafe(i % nTicksPerBatch, (double) decoder.getValue());

        } else if (value instanceof BigDecimal) {
          ((BigIntVector) root.getVector(BIGDECIMAL_VALUES_KEY)).setSafe(i % nTicksPerBatch, ((BigDecimal) value).longValue());
          ((BigIntVector) root.getVector(BIGDECIMAL_SCALES_KEY)).setSafe(i % nTicksPerBatch, ((BigDecimal) value).scale());

        } else if (value instanceof String) {

          if (decoder.isBinary()) {
            ((VarCharVector) root.getVector(STRING_VALUES_KEY)).setSafe(i % nTicksPerBatch, new Text((String) value));

          } else {
            ((VarBinaryVector) root.getVector(BYTES_VALUES_KEY)).setSafe(i % nTicksPerBatch, ((String) value).getBytes());
          }

        } else {
          throw new WarpScriptException("Unrecognized value type when trying to convert a GTSENCODER to an Arrow Stream");
        }

        if (i % nTicksPerBatch == nTicksPerBatch - 1) {
          writer.writeBatch();
        }

      }

      if ((encoder.size() - 1) % nTicksPerBatch != nTicksPerBatch - 1 ) {
        root.setRowCount((encoder.size() - 1) % nTicksPerBatch);
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
  // Readers
  //

  public static Object fromArrowStream(ReadableByteChannel in) throws WarpScriptException {

    Object res = null;

    try (ArrowStreamReader reader = new ArrowStreamReader(in, new RootAllocator(Integer.MAX_VALUE))) {

      //
      // Check types by reading schema
      //

      VectorSchemaRoot root = reader.getVectorSchemaRoot();

      if (TYPEOF.typeof(GeoTimeSerie.class) != root.getSchema().getCustomMetadata().get(TYPE)) {
        res = arrowStreamToGTS(root, reader);

      } else if (TYPEOF.typeof(GTSEncoder.class) != root.getSchema().getCustomMetadata().get(TYPE)) {
        res = arrowStreamToGtsEncoder(root, reader);

      } else {

        //TODO(jc): should return metadata and a map of list (per field)

        throw new WarpScriptException("Unsupported type right now.");
      }

    } catch (IOException ioe) {
      throw new WarpScriptException(ioe);
    }

   return res;
  }

  private static void safeSetType(GeoTimeSerie gts, GeoTimeSerie.TYPE type) throws WarpScriptException {
    if (GeoTimeSerie.TYPE.UNDEFINED != gts.getType()) {
      throw new WarpScriptException("Tried to set type of a GTS that already has a type.");
    } else {
      gts.setType(type);
    }
  }

  private static GeoTimeSerie arrowStreamToGTS(VectorSchemaRoot root, ArrowStreamReader reader) throws IOException, WarpScriptException {

    Schema schema = root.getSchema();
    if (TYPEOF.typeof(GeoTimeSerie.class) != schema.getCustomMetadata().get(TYPE)) {
      throw new WarpScriptException("Not a Geo Time Series.");
    }

    GeoTimeSerie gts =  new GeoTimeSerie();
    gts.setMetadata(retrieveGtsMetadata(schema));

    //
    // Retrieve fields
    //

    FieldVector timestampField = root.getVector(TIMESTAMPS_KEY);
    FieldVector latitudeField =  root.getVector(LATITUDE_KEY);
    FieldVector longitudeField =  root.getVector(LONGITUDE_KEY);
    FieldVector elevationField =  root.getVector(ELEVATION_KEY);
    FieldVector longField =  root.getVector(LONG_VALUES_KEY);
    FieldVector doubleField =  root.getVector(DOUBLE_VALUES_KEY);
    FieldVector booleanField =  root.getVector(BOOLEAN_VALUES_KEY);
    FieldVector stringField =  root.getVector(STRING_VALUES_KEY);

    //
    // Set Gts type
    //

    if (null != longField) {
      safeSetType(gts, GeoTimeSerie.TYPE.LONG);
    }

    if (null != doubleField) {
      safeSetType(gts, GeoTimeSerie.TYPE.DOUBLE);
    }

    if (null != booleanField) {
      safeSetType(gts, GeoTimeSerie.TYPE.BOOLEAN);
    }

    if (null != stringField) {
      safeSetType(gts, GeoTimeSerie.TYPE.STRING);
    }

    //
    // Retrieve time unit per seconds
    //

    long stu = Long.valueOf(schema.getCustomMetadata().get(STU)).longValue();
    double timeFactor = new Double(stu) / Constants.TIME_UNITS_PER_S;

    //
    // Retrieve bucketize info
    //

    if (null != schema.getCustomMetadata().get(LASTBUCKET)) {
      GTSHelper.setLastBucket(gts, Integer.valueOf(schema.getCustomMetadata().get(LASTBUCKET)).intValue());
    }

    if (null != schema.getCustomMetadata().get(BUCKETSPAN)) {
      GTSHelper.setBucketSpan(gts, Integer.valueOf(schema.getCustomMetadata().get(BUCKETSPAN)).intValue());
    }

    if (null != schema.getCustomMetadata().get(BUCKETCOUNT)) {
      GTSHelper.setBucketCount(gts, Integer.valueOf(schema.getCustomMetadata().get(BUCKETCOUNT)).intValue());
    }

    //
    // Read data points
    //

    while (reader.loadNextBatch()) {

      for (int i = 0; i < root.getRowCount(); i++) {

        timestampField.getReader().setPosition(i);
        long tick = timestampField.getReader().readLong().longValue();
        if (timeFactor != 1.0D) {
          tick = new Double(tick * timeFactor).longValue();
        }

        long location = GeoTimeSerie.NO_LOCATION;
        if (null != latitudeField && null != longitudeField) {
          latitudeField.getReader().setPosition(i);
          longitudeField.getReader().setPosition(i);
          location = GeoXPLib.toGeoXPPoint(latitudeField.getReader().readFloat(), longitudeField.getReader().readFloat());
        }

        long elevation = GeoTimeSerie.NO_ELEVATION;
        if (null != elevationField) {
          elevationField.getReader().setPosition(i);
          elevation = elevationField.getReader().readLong();
        }

        Object value;
        switch (gts.getType()) {
          case LONG:
            longField.getReader().setPosition(i);
            value = longField.getReader().readObject();
            break;

          case DOUBLE:
            doubleField.getReader().setPosition(i);
            value = doubleField.getReader().readObject();
            break;

          case BOOLEAN:
            booleanField.getReader().setPosition(i);
            value = booleanField.getReader().readObject();
            break;

          case STRING:
            stringField.getReader().setPosition(i);
            value = stringField.getReader().readObject();
            break;

          default: throw new WarpScriptException("Can't define GTS type of input arrow stream");
        }

        GTSHelper.setValue(gts, tick, location, elevation, value, false);
      }
    }

    return gts;
  }

  private static GTSEncoder arrowStreamToGtsEncoder(VectorSchemaRoot root, ArrowStreamReader reader) throws IOException, WarpScriptException {
    throw new WarpScriptException("Not yet implemented");
  }



}
