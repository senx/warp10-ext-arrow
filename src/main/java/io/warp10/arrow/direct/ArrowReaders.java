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
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.WarpScriptException;
import io.warp10.script.functions.TYPEOF;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.boon.json.JsonParser;
import org.boon.json.JsonParserFactory;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.warp10.arrow.direct.ArrowHelper.BOOLEAN_VALUES_KEY;
import static io.warp10.arrow.direct.ArrowHelper.BUCKETCOUNT;
import static io.warp10.arrow.direct.ArrowHelper.BUCKETSPAN;
import static io.warp10.arrow.direct.ArrowHelper.BYTES_VALUES_KEY;
import static io.warp10.arrow.direct.ArrowHelper.DOUBLE_VALUES_KEY;
import static io.warp10.arrow.direct.ArrowHelper.ELEVATION_KEY;
import static io.warp10.arrow.direct.ArrowHelper.LASTBUCKET;
import static io.warp10.arrow.direct.ArrowHelper.LATITUDE_KEY;
import static io.warp10.arrow.direct.ArrowHelper.LONGITUDE_KEY;
import static io.warp10.arrow.direct.ArrowHelper.LONG_VALUES_KEY;
import static io.warp10.arrow.direct.ArrowHelper.STRING_VALUES_KEY;
import static io.warp10.arrow.direct.ArrowHelper.STU;
import static io.warp10.arrow.direct.ArrowHelper.TIMESTAMPS_KEY;
import static io.warp10.arrow.direct.ArrowHelper.TYPE;

public class ArrowReaders {

  //
  // Json converters for labels and attributes
  //

  final static JsonParser parser = new JsonParserFactory().create();
  private static Object fromJson(String s) {
    return parser.parse(s);
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
      GTSHelper.setLastBucket(gts, Long.valueOf(schema.getCustomMetadata().get(LASTBUCKET)).intValue());
    }

    if (null != schema.getCustomMetadata().get(BUCKETSPAN)) {
      GTSHelper.setBucketSpan(gts, Long.valueOf(schema.getCustomMetadata().get(BUCKETSPAN)).intValue());
    }

    if (null != schema.getCustomMetadata().get(BUCKETCOUNT)) {
      GTSHelper.setBucketCount(gts, Long.valueOf(schema.getCustomMetadata().get(BUCKETCOUNT)).intValue());
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
          if (null == res.get(name)) {
            res.put(name, new ArrayList<String>());
          }

          root.getVector(name).getReader().setPosition(i);
          if (!root.getVector(name).getReader().isSet()) {
            res.get(name).add(null);

          } else {

            switch (field.getFieldType().getType().getTypeID()) {

              case Int:

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

                switch (((ArrowType.FloatingPoint) field.getFieldType().getType()).getPrecision()) {
                  case HALF:
                    if (true)
                      throw new WarpScriptException("Floating point precision other than 32 or 64 bits are not supported.");
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

                res.get(name).add(root.getVector(name).getReader().readText().toString());
                break;

              case Binary:

                res.get(name).add(Base64.getEncoder().encodeToString(root.getVector(name).getReader().readByteArray()));
                break;

              case Bool:

                res.get(name).add(root.getVector(name).getReader().readByte() == 1);
                break;

              case FixedSizeBinary:
              case Decimal:
                if (true)
                  throw new WarpScriptException(field.getFieldType().getType().getTypeID().name() + " Arrow type not yet supported"); // TODO
                break;

              case Date:
              case Time:
              case Timestamp:
              case Interval:
              case Duration:
                if (true)
                  throw new WarpScriptException(field.getFieldType().getType().getTypeID().name() + " Arrow type not yet supported"); // maybe should support ?
                break;

              case Null:
              case Struct:
              case List:
              case FixedSizeList:
              case Union:
              case Map:
              case NONE:
                if (true)
                  throw new WarpScriptException(field.getFieldType().getType().getTypeID().name() + " Arrow type not supported");
                break;
            }
          }
        }
      }
    }
    return  res;
  }
}
