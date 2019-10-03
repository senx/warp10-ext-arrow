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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
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
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utilities and converters
 *
 * TODO(JC): maybe refactor into multiple classes (one reader/writer per subtype and type).
 */
public class ArrowVectorHelper {

  public final static String TIMESTAMPS_KEY = "timestamp";
  public final static String LONG_VALUES_KEY = TYPEOF.typeof(Long.class);
  public final static String DOUBLE_VALUES_KEY = TYPEOF.typeof(Double.class);
  //public final static String BIGDECIMAL_VALUES_KEY = "BIGDECIMAL.CONTENT";
  //public final static String BIGDECIMAL_SCALES_KEY = "BIGDECIMAL.SCALE";
  public final static String BOOLEAN_VALUES_KEY = TYPEOF.typeof(Boolean.class);
  public final static String STRING_VALUES_KEY = TYPEOF.typeof(String.class);
  public final static String BYTES_VALUES_KEY = TYPEOF.typeof(byte[].class);
  public final static String LATITUDE_KEY = "latitude";
  public final static String LONGITUDE_KEY = "longitude";
  public final static String ELEVATION_KEY = "elevation";

  public final static String BUCKETSPAN = "bucketspan";
  public final static String BUCKETCOUNT = "bucketcount";
  public final static String LASTBUCKET = "lastbucket";

  public final static String TYPE = "WarpScriptType";
  public final static String REV = "WarpScriptVersion";
  public final static String STU = "WarpScriptTimeUnitsPerSecond";

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
  final static Field STRING_VALUES_FIELD = Field.nullable(STRING_VALUES_KEY, new ArrowType.Utf8());
  final static Field LATITUDE_FIELD = Field.nullable(LATITUDE_KEY, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
  final static Field LONGITUDE_FIELD = Field.nullable(LONGITUDE_KEY, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
  final static Field ELEVATION_FIELD = Field.nullable(ELEVATION_KEY, new ArrowType.Int(64, true));

  // additional fields for GTSEncoders
  final static Field BYTES_VALUES_FIELD = Field.nullable(BYTES_VALUES_KEY, new ArrowType.Binary());

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

  /**
   * Retrieve Gts Metadata from an Arrow Schema
   * @param schema
   * @return
   */
  public static Metadata retrieveGtsMetadata(Schema schema) {

    Metadata gtsMeta = new Metadata();
    Map<String, String> metadata = schema.getCustomMetadata();

    gtsMeta.setName(metadata.get(Metadata._Fields.NAME.getFieldName()));

    String labels = metadata.get(Metadata._Fields.LABELS.getFieldName());
    if (null != labels) {
      gtsMeta.setLabels((Map<String, String>) fromJson(labels));
    }

    String classId = metadata.get(Metadata._Fields.CLASS_ID.getFieldName());
    if (null != classId) {
      gtsMeta.setClassId(Long.valueOf(classId).longValue());
    }

    String labelsId = metadata.get(Metadata._Fields.LABELS_ID.getFieldName());
    if (null != labelsId) {
      gtsMeta.setLabelsId(Long.valueOf(labelsId).longValue());
    }

    String attributes = metadata.get(Metadata._Fields.ATTRIBUTES.getFieldName());
    if (null != attributes) {
      gtsMeta.setAttributes((Map<String, String>) fromJson(attributes));
    }

    gtsMeta.setSource(metadata.get(Metadata._Fields.SOURCE.getFieldName()));

    String lastActivity = metadata.get(Metadata._Fields.LAST_ACTIVITY.getFieldName());
    if (null != lastActivity) {
      gtsMeta.setLastActivity(Long.valueOf(lastActivity).longValue());
    }

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

    if (gts.size() == 0) {
      return;
    }

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

  //
  // Readers
  //

  public static Object fromArrowStream(ReadableByteChannel in, boolean mapListOutput) throws WarpScriptException {

    Object res = null;

    try (ArrowStreamReader reader = new ArrowStreamReader(in, new RootAllocator(Integer.MAX_VALUE))) {

      //
      // Check types by reading schema
      //

      VectorSchemaRoot root = reader.getVectorSchemaRoot();

      if (!mapListOutput && TYPEOF.typeof(GeoTimeSerie.class).equals(root.getSchema().getCustomMetadata().get(TYPE))) {
        res = arrowStreamToGTS(reader);

      } else if (!mapListOutput && TYPEOF.typeof(GTSEncoder.class).equals(root.getSchema().getCustomMetadata().get(TYPE))) {
        res = arrowStreamToGtsEncoder(reader);

      } else {

        res = new ArrayList<>();
        ((ArrayList) res).add(root.getSchema().getCustomMetadata());
        ((ArrayList) res).add(arrowStreamToMapOfLists(reader));
      }

    } catch (IOException ioe) {
      throw new WarpScriptException(ioe);
    }

   return res;
  }

  static void safeSetType(GeoTimeSerie gts, GeoTimeSerie.TYPE type) throws WarpScriptException {
    if (GeoTimeSerie.TYPE.UNDEFINED != gts.getType()) {
      throw new WarpScriptException("Tried to set type of a GTS that already has a type.");
    } else {
      gts.setType(type);
    }
  }

  public static GeoTimeSerie arrowStreamToGTS(ArrowStreamReader reader) throws IOException, WarpScriptException {

    VectorSchemaRoot root = reader.getVectorSchemaRoot();
    Schema schema = root.getSchema();
    if (!TYPEOF.typeof(GeoTimeSerie.class).equals(schema.getCustomMetadata().get(TYPE))) {
      throw new WarpScriptException("Tried to convert a GTS but input is not a GTS.");
    }

    GeoTimeSerie gts =  new GeoTimeSerie();
    gts.setMetadata(retrieveGtsMetadata(schema));

    //
    // Retrieve fields
    //

    FieldVector timestampVector = root.getVector(TIMESTAMPS_KEY);
    FieldVector latitudeVector =  root.getVector(LATITUDE_KEY);
    FieldVector longitudeVector =  root.getVector(LONGITUDE_KEY);
    FieldVector elevationVector =  root.getVector(ELEVATION_KEY);
    FieldVector longVector =  root.getVector(LONG_VALUES_KEY);
    FieldVector doubleVector =  root.getVector(DOUBLE_VALUES_KEY);
    FieldVector booleanVector =  root.getVector(BOOLEAN_VALUES_KEY);
    FieldVector stringVector =  root.getVector(STRING_VALUES_KEY);

    //
    // Set Gts type
    //

    if (null != longVector) {
      safeSetType(gts, GeoTimeSerie.TYPE.LONG);
    }

    if (null != doubleVector) {
      safeSetType(gts, GeoTimeSerie.TYPE.DOUBLE);
    }

    if (null != booleanVector) {
      safeSetType(gts, GeoTimeSerie.TYPE.BOOLEAN);
    }

    if (null != stringVector) {
      safeSetType(gts, GeoTimeSerie.TYPE.STRING);
    }

    //
    // Retrieve time unit per seconds
    //

    Object stu_holder = schema.getCustomMetadata().get(STU);
    long stu = stu_holder != null ? Long.valueOf((String) stu_holder).longValue() : Constants.TIME_UNITS_PER_S;
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

        timestampVector.getReader().setPosition(i);
        long tick = timestampVector.getReader().readLong().longValue();
        if (timeFactor != 1.0D) {
          tick = new Double(tick * timeFactor).longValue();
        }

        long location = GeoTimeSerie.NO_LOCATION;
        if (null != latitudeVector && null != longitudeVector) {
          latitudeVector.getReader().setPosition(i);
          longitudeVector.getReader().setPosition(i);
          location = GeoXPLib.toGeoXPPoint(latitudeVector.getReader().readFloat(), longitudeVector.getReader().readFloat());
        }

        long elevation = GeoTimeSerie.NO_ELEVATION;
        if (null != elevationVector) {
          elevationVector.getReader().setPosition(i);
          elevation = elevationVector.getReader().readLong();
        }

        Object value;
        switch (gts.getType()) {
          case LONG:
            longVector.getReader().setPosition(i);
            value = longVector.getReader().readObject();
            break;

          case DOUBLE:
            doubleVector.getReader().setPosition(i);
            value = doubleVector.getReader().readObject();
            break;

          case BOOLEAN:
            booleanVector.getReader().setPosition(i);
            value = booleanVector.getReader().readObject();
            break;

          case STRING:
            stringVector.getReader().setPosition(i);
            value = stringVector.getReader().readObject();
            break;

          default: throw new WarpScriptException("Can't define GTS type of input arrow stream");
        }

        GTSHelper.setValue(gts, tick, location, elevation, value, false);
      }
    }

    return gts;
  }

  private enum ENCODER_VALUE_TYPE{
    LONG,
    DOUBLE,
    //BIG_DECIMAL,
    BOOLEAN,
    UTF8,
    BINARY
  }

  public static GTSEncoder arrowStreamToGtsEncoder(ArrowStreamReader reader) throws IOException, WarpScriptException {

    VectorSchemaRoot root = reader.getVectorSchemaRoot();
    Schema schema = root.getSchema();
    if (!TYPEOF.typeof(GTSEncoder.class).equals(schema.getCustomMetadata().get(TYPE))) {
      throw new WarpScriptException("Tried to convert a GTSENCODER but input is not a GTSENCODER.");
    }

    GTSEncoder encoder =  new GTSEncoder();
    encoder.setMetadata(retrieveGtsMetadata(schema));

    //
    // Retrieve fields
    //

    FieldVector timestampVector = root.getVector(TIMESTAMPS_KEY);
    FieldVector latitudeVector =  root.getVector(LATITUDE_KEY);
    FieldVector longitudeVector =  root.getVector(LONGITUDE_KEY);
    FieldVector elevationVector =  root.getVector(ELEVATION_KEY);
    FieldVector longVector =  root.getVector(LONG_VALUES_KEY);
    FieldVector doubleVector =  root.getVector(DOUBLE_VALUES_KEY);
    //FieldVector bigDecimalValuesVector =  root.getVector(BIGDECIMAL_VALUES_KEY);
    //FieldVector bigDecimalScalesVector =  root.getVector(BIGDECIMAL_SCALES_KEY);
    FieldVector booleanVector =  root.getVector(BOOLEAN_VALUES_KEY);
    FieldVector utf8Vector =  root.getVector(STRING_VALUES_KEY);
    FieldVector bytesVector =  root.getVector(BYTES_VALUES_KEY);

    //
    // Retrieve time unit per seconds
    //

    Object stu_holder = schema.getCustomMetadata().get(STU);
    long stu = stu_holder != null ? Long.valueOf((String) stu_holder).longValue() : Constants.TIME_UNITS_PER_S;
    double timeFactor = new Double(stu) / Constants.TIME_UNITS_PER_S;

    //
    // Read data points
    //

    while (reader.loadNextBatch()) {

      for (int i = 0; i < root.getRowCount(); i++) {

        timestampVector.getReader().setPosition(i);

        if (null == timestampVector.getReader().readLong()) {
          throw new WarpScriptException("Failed index: " + i);
        }


        long tick = timestampVector.getReader().readLong().longValue();
        if (timeFactor != 1.0D) {
          tick = new Double(tick * timeFactor).longValue();
        }

        long location = GeoTimeSerie.NO_LOCATION;
        if (null != latitudeVector && null != longitudeVector) {
          latitudeVector.getReader().setPosition(i);
          longitudeVector.getReader().setPosition(i);
          location = GeoXPLib.toGeoXPPoint(latitudeVector.getReader().readFloat(), longitudeVector.getReader().readFloat());
        }

        long elevation = GeoTimeSerie.NO_ELEVATION;
        if (null != elevationVector) {
          elevationVector.getReader().setPosition(i);
          elevation = elevationVector.getReader().readLong();
        }

        //
        // Value
        //

        ENCODER_VALUE_TYPE type = null;
        Object value;
        Object read;

        longVector.getReader().setPosition(i);
        value = longVector.getReader().readObject();
        if (null != value) {
          type = ENCODER_VALUE_TYPE.LONG;
        }

        doubleVector.getReader().setPosition(i);
        read = doubleVector.getReader().readObject();
        if (null != read) {
          if (null != type) {
            throw new WarpScriptException("GTS encoder received a non binary value with multiple types.");
          }

          value = read;
          type = ENCODER_VALUE_TYPE.DOUBLE;
        }

        doubleVector.getReader().setPosition(i);
        read = doubleVector.getReader().readObject();
        if (null != read) {
          if (null != type) {
            throw new WarpScriptException("GTS encoder received a non binary value with multiple types.");
          }

          value = read;
          type = ENCODER_VALUE_TYPE.DOUBLE;
        }

        booleanVector.getReader().setPosition(i);
        read = booleanVector.getReader().readObject();
        if (null != read) {
          if (null != type) {
            throw new WarpScriptException("GTS encoder received a non binary value with multiple types.");
          }

          value = read;
          type = ENCODER_VALUE_TYPE.BOOLEAN;
        }

        utf8Vector.getReader().setPosition(i);
        read = utf8Vector.getReader().readObject();
        if (null != read) {
          if (null != type) {
            throw new WarpScriptException("GTS encoder received a non binary value with multiple types.");
          }

          value = read;
          type = ENCODER_VALUE_TYPE.UTF8;
        }

        bytesVector.getReader().setPosition(i);
        read = bytesVector.getReader().readObject();
        if (null != read) {
          if (null != type) {
            throw new WarpScriptException("GTS encoder received a non binary value with multiple types.");
          }

          value = read;
          type = ENCODER_VALUE_TYPE.BINARY;
        }

        if (null == value) {
          throw new WarpScriptException("Can not define type of encoded value.");
        }

        encoder.addValue(tick, location, elevation, value);
      }
    }

    return encoder;
  }

  public static Map<String, List> arrowStreamToMapOfLists(ArrowStreamReader reader) throws IOException, WarpScriptException {

    VectorSchemaRoot root = reader.getVectorSchemaRoot();
    Map<String, List> res = new HashMap<String, List>();
    Schema schema = root.getSchema();
    Map<Long, Dictionary> dictionaries = reader.getDictionaryVectors();

    while (reader.loadNextBatch()) {

      for (int i = 0; i < root.getRowCount(); i++) {

        for (Field field: schema.getFields()) {
          String name = field.getName();
          root.getVector(name).getReader().setPosition(i);

          switch (field.getFieldType().getType().getTypeID()) {

            case Int:
              if (null == res.get(name)) {
                res.put(name, new ArrayList<Long>());
              }

              if (!((ArrowType.Int) field.getFieldType().getType()).getIsSigned()) {
                throw new RuntimeException("Unsigned int not supported.");
              }

              long val;
              int bitWidth = ((ArrowType.Int) field.getFieldType().getType()).getBitWidth();
              if (64 == bitWidth) {
                val = root.getVector(name).getReader().readLong().longValue();
              } else if (32 == bitWidth) {
                val = root.getVector(name).getReader().readInteger().longValue();
              } else if (16 == bitWidth) {
                val = root.getVector(name).getReader().readShort().longValue();
              } else {
                throw new WarpScriptException("Int bit width other than 16, 32 or 64 are not supported.");
              }

              DictionaryEncoding encoding = field.getDictionary();
              if (null == encoding) {
                res.get(name).add(val);
              } else {
                if (!dictionaries.get(encoding.getId()).getVectorType().getTypeID().equals(ArrowType.Utf8.TYPE_TYPE)) {
                  throw new WarpScriptException("Dictionary encoding only support String values.");
                }

                FieldVector vector = dictionaries.get(encoding.getId()).getVector();
                vector.getReader().setPosition((int) val);
                res.get(name).add(vector.getReader().readText().toString());

              }

              break;

            case FloatingPoint:

              if (null == res.get(name)) {
                res.put(name, new ArrayList<Double>());
              }
              switch (((ArrowType.FloatingPoint) field.getFieldType().getType()).getPrecision()) {
                case HALF:
                  if (true) throw new WarpScriptException("Floating point precision other than 32 or 64 bits are not supported.");
                  break;

                case SINGLE: // 32-bit
                  res.get(name).add(root.getVector(name).getReader().readFloat().doubleValue());
                  break;

                case DOUBLE: // 64-bit
                  res.get(name).add(root.getVector(name).getReader().readDouble().doubleValue());
                  break;
              }
              break;

            case Utf8:

              if (null == res.get(name)) {
                res.put(name, new ArrayList<String>());
              }
              res.get(name).add(root.getVector(name).getReader().readText().toString());
              break;

            case Binary:

              if (null == res.get(name)) {
                res.put(name, new ArrayList<String>());
              }
              res.get(name).add(Base64.getEncoder().encodeToString(root.getVector(name).getReader().readByteArray()));
              break;

            case Bool:

              if (null == res.get(name)) {
                res.put(name, new ArrayList<String>());
              }
              res.get(name).add(root.getVector(name).getReader().readByte() == 1);
              break;

            case FixedSizeBinary:
            case Decimal:
              if (true) throw new WarpScriptException(field.getFieldType().getType().getTypeID().name() + " Arrow type not yet supported"); // TODO
              break;

            case Date:
            case Time:
            case Timestamp:
            case Interval:
            case Duration:
              if (true) throw new WarpScriptException(field.getFieldType().getType().getTypeID().name() + " Arrow type not yet supported"); // maybe should support ?
              break;

            case Null:
            case Struct:
            case List:
            case FixedSizeList:
            case Union:
            case Map:
            case NONE:
              if (true) throw new WarpScriptException(field.getFieldType().getType().getTypeID().name() + " Arrow type not supported");
              break;
          }
        }
      }
    }
    return  res;
  }
}
