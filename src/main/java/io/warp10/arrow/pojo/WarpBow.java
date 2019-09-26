//
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

package io.warp10.arrow.pojo;

import com.geoxp.GeoXPLib;
import io.warp10.Revision;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.WarpScriptException;
import io.warp10.script.functions.TYPEOF;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A WarpBow build the Arrow schema corresponding to WarpScript logics,
 * also handle stream writers and readers.
 */
public class WarpBow {

  public final static String TYPE = "WarpScriptType";
  public final static String REV = "WarpScriptVersion";
  public final static String STU = "WarpScriptTimeUnitsPerSecond";

  final private VectorSchemaRoot root;
  final private Schema schema;
  final private BufferAllocator allocator;
  final private Map<String, String> metadata;
  final private List<WarpField> warpFields;
  final private DictionaryProvider.MapDictionaryProvider dictionaryProvider;
  final private Object[] dataPointHolder;

  public Schema getSchema() {
    return schema;
  }

  public VectorSchemaRoot getRoot() {
    return root;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public List<WarpField> getWarpFields() {
    return warpFields;
  }

  public DictionaryProvider getDictionaryProvider() {
    return dictionaryProvider;
  }

  public WarpBow(Map<String, String> metadata, List<WarpField> warpFields) {

    this.metadata = metadata;
    this.warpFields = warpFields;
    allocator = new RootAllocator();
    dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();

    List<Field> fields = new ArrayList<Field>(warpFields.size());
    List<FieldVector> vectors = new ArrayList<FieldVector>(warpFields.size());

    for(WarpField warpField: warpFields) {
      fields.add(warpField.getField());
      warpField.initialize(allocator);
      vectors.add(warpField.getVector());

      if (warpField instanceof DictionaryEncodedWarpField) {
        dictionaryProvider.put(((DictionaryEncodedWarpField) warpField).getDictionary());
      }
    }

    schema = new Schema(fields, metadata);
    root = new VectorSchemaRoot(schema, vectors, 0);
    dataPointHolder = new Object[warpFields.size()];
    for (int i = 0; i < dataPointHolder.length; i++) {
      dataPointHolder[i] = null;
    }
  }

  public WarpBow(List<WarpField> warpFields) {
    this(null, warpFields);
  }

  public void clear() {
    for(WarpField warpField: warpFields) {
      warpField.clear();
    }
  }

  public void clearDictionaries() {
    for(WarpField warpField: warpFields) {
      if (warpField instanceof DictionaryEncodedWarpField) {
        ((DictionaryEncodedWarpField) warpField).clearDictionary();
      }
    }
  }

  public void set(int index, Object[] o) {
    int i = 0;
    for(WarpField warpField: warpFields) {
      warpField.setSafe(index, o[i++]);
    }
  }

  public static WarpBow singleGtsSchema(GeoTimeSerie gts) throws WarpScriptException {
    throw new WarpScriptException("Not yet implemented. Please use ArrowVectorHelper's equivalent method for now.");
  }

  public static WarpBow singleGtsEncoderSchema(GTSEncoder encoder) throws WarpScriptException {
    throw new WarpScriptException("Not yet implemented. Please use ArrowVectorHelper's equivalent method for now.");
  }

  /**
   * Build a WarpSchema from a list containing GTS and GtsEncoders.
   * Only necessary fields are added, in the order: meta-, time-, geo-, value- fields.
   *
   * @param list
   * @return
   * @throws WarpScriptException
   */
  public static WarpBow GtsOrEncoderListSchema(List<Object> list) throws WarpScriptException {
    for (Object o: list) {
      if (!(o instanceof GeoTimeSerie) || !(o instanceof GTSEncoder)) {
        throw new WarpScriptException("Input list should contain only GTS or GTSENCODER.");
      }
    }

    //
    // We loop through the list to choose which non-empty fields to add to the schema.
    //

    List<String> namePool = new ArrayList<>(); // The pool of fields already picked by name
    List<WarpField> fields = new ArrayList<>(); // The fields to make up for a schema
    int nLabelsOrAttributes = 0;

    //
    // Metadata fields (classname, labels and attributes)
    //

    for (Object o: list) {

      if (o instanceof GeoTimeSerie) {
        GeoTimeSerie gts = (GeoTimeSerie) o;

        if (!namePool.contains(ClassnameWarpField.CLASSNAME_KEY)) {
          fields.add(new ClassnameWarpField());
          namePool.add(ClassnameWarpField.CLASSNAME_KEY);
        }

        Map<String, String> labels = gts.getLabels();
        for (String key: labels.keySet()) {

          if(!namePool.contains(key)) {
            fields.add(new LabelWarpField(key, ++nLabelsOrAttributes, LabelWarpField.Type.LABEL)); // id 0 is reserved for classname if field is used
            namePool.add(key);
          }
        }

        Map<String, String> attributes = gts.getMetadata().getAttributes();
        for (String key: attributes.keySet()) {

          if(!namePool.contains(key)) {
            fields.add(new LabelWarpField(key, ++nLabelsOrAttributes, LabelWarpField.Type.ATTRIBUTE));
            namePool.add(key);
          }
        }

      } else if (o instanceof GTSEncoder) {
        GTSEncoder encoder = (GTSEncoder) o;

        if (encoder.getName().length() > 0) {
          if (!namePool.contains(ClassnameWarpField.CLASSNAME_KEY)) {
            fields.add(new ClassnameWarpField());
            namePool.add(ClassnameWarpField.CLASSNAME_KEY);
          }
        }

        if (null != encoder.getRawMetadata()) {

          Map<String, String> labels = encoder.getLabels();
          for (String key: labels.keySet()) {

            if(!namePool.contains(key)) {
              fields.add(new LabelWarpField(key, ++nLabelsOrAttributes, LabelWarpField.Type.LABEL)); // id 0 is reserved for classname if field is used
              namePool.add(key);
            }
          }

          Map<String, String> attributes = encoder.getMetadata().getAttributes();
          for (String key: attributes.keySet()) {

            if(!namePool.contains(key)) {
              fields.add(new LabelWarpField(key, ++nLabelsOrAttributes, LabelWarpField.Type.ATTRIBUTE));
              namePool.add(key);
            }
          }
        }
      }
    }

    //
    // Timestamp fields (mandatory)
    //

    fields.add(new TimestampWarpField());

    //
    // Geo fields
    //

    for (Object o: list) {

      if (o instanceof GeoTimeSerie) {
        GeoTimeSerie gts = (GeoTimeSerie) o;

        if (gts.hasLocations()) {
          if(!namePool.contains(LatitudeWarpField.LATITUDE_KEY)) {
            fields.add(new LatitudeWarpField());
            namePool.add(LatitudeWarpField.LATITUDE_KEY);
          }

          if(!namePool.contains(LongitudeWarpField.LONGITUDE_KEY)) {
            fields.add(new LongitudeWarpField());
            namePool.add(LongitudeWarpField.LONGITUDE_KEY);
          }
        }

        if (gts.hasElevations()) {
          if(!namePool.contains(ElevationWarpField.ELEVATION_KEY)) {
            fields.add(new ElevationWarpField());
            namePool.add(ElevationWarpField.ELEVATION_KEY);
          }
        }

      } else if (o instanceof GTSEncoder) {
        GTSEncoder encoder = (GTSEncoder) o;

        //
        // Here we must add every fields since we don't know yet if they would be empty
        //

        if(!namePool.contains(LatitudeWarpField.LATITUDE_KEY)) {
          fields.add(new LatitudeWarpField());
          namePool.add(LatitudeWarpField.LATITUDE_KEY);
        }

        if(!namePool.contains(LongitudeWarpField.LONGITUDE_KEY)) {
          fields.add(new LongitudeWarpField());
          namePool.add(LongitudeWarpField.LONGITUDE_KEY);
        }

        if(!namePool.contains(ElevationWarpField.ELEVATION_KEY)) {
          fields.add(new ElevationWarpField());
          namePool.add(ElevationWarpField.ELEVATION_KEY);
        }
      }
    }

    //
    // Value fields
    //

    for (Object o: list) {

      if (o instanceof GeoTimeSerie) {
        GeoTimeSerie gts = (GeoTimeSerie) o;

        switch (gts.getType()) {
          case LONG:
            if(!namePool.contains(ValueWarpField.LONG_VALUES_KEY)) {
              fields.add(new ValueWarpField(ValueWarpField.Type.LONG));
              namePool.add(ValueWarpField.LONG_VALUES_KEY);
            }
            break;

          case DOUBLE:
            if(!namePool.contains(ValueWarpField.DOUBLE_VALUES_KEY)) {
              fields.add(new ValueWarpField(ValueWarpField.Type.DOUBLE));
              namePool.add(ValueWarpField.DOUBLE_VALUES_KEY);
            }
            break;

          case BOOLEAN:
            if(!namePool.contains(ValueWarpField.BOOLEAN_VALUES_KEY)) {
              fields.add(new ValueWarpField(ValueWarpField.Type.BOOLEAN));
              namePool.add(ValueWarpField.BOOLEAN_VALUES_KEY);
            }
            break;

          case STRING:
            if(!namePool.contains(ValueWarpField.STRING_VALUES_KEY)) {
              fields.add(new ValueWarpField(ValueWarpField.Type.STRING));
              namePool.add(ValueWarpField.STRING_VALUES_KEY);
            }
            break;
        }

      } else if (o instanceof GTSEncoder) {
        GTSEncoder encoder = (GTSEncoder) o;

        if (encoder.getCount() > 0) {

          //
          // Here we must add every possible field since we don't know yet the values
          //

          if(!namePool.contains(ValueWarpField.LONG_VALUES_KEY)) {
            fields.add(new ValueWarpField(ValueWarpField.Type.LONG));
            namePool.add(ValueWarpField.LONG_VALUES_KEY);
          }
          if(!namePool.contains(ValueWarpField.DOUBLE_VALUES_KEY)) {
            fields.add(new ValueWarpField(ValueWarpField.Type.DOUBLE));
            namePool.add(ValueWarpField.DOUBLE_VALUES_KEY);
          }
          if(!namePool.contains(ValueWarpField.BOOLEAN_VALUES_KEY)) {
            fields.add(new ValueWarpField(ValueWarpField.Type.BOOLEAN));
            namePool.add(ValueWarpField.BOOLEAN_VALUES_KEY);
          }
          if(!namePool.contains(ValueWarpField.STRING_VALUES_KEY)) {
            fields.add(new ValueWarpField(ValueWarpField.Type.STRING));
            namePool.add(ValueWarpField.STRING_VALUES_KEY);
          }
          if(!namePool.contains(ValueWarpField.BYTES_VALUES_KEY)) {
            fields.add(new ValueWarpField(ValueWarpField.Type.BYTES));
            namePool.add(ValueWarpField.BYTES_VALUES_KEY);
          }
        }
      }
    }

    Map<String, String> metadata = new HashMap<String, String>(2);
    metadata.put(TYPE, TYPEOF.typeof(List.class));
    metadata.put(REV, Revision.REVISION);
    metadata.put(STU, String.valueOf(Constants.TIME_UNITS_PER_S));

    return new WarpBow(metadata, fields);
  }

  /**
   * Populate dataPointHolder with data from input GTS at given index in order given by the schema.
   *
   * @param index
   * @param gts
   */
  public void prepareGtsDataPoint(int index, GeoTimeSerie gts) throws WarpScriptException {

    double[] geoPointHolder = null;
    if (gts.hasElevations()) {
      long location = GTSHelper.locationAtIndex(gts, index);
      if (GeoTimeSerie.NO_LOCATION != location) {
        geoPointHolder = GeoXPLib.fromGeoXPPoint(location);
      }
    }

    for (int i = 0; i < warpFields.size(); i++) {
      WarpField warpField = warpFields.get(i);

      if (warpField instanceof ClassnameWarpField) {
        dataPointHolder[i] = gts.getName();

      } else if (warpField instanceof LabelWarpField) {

        switch (((LabelWarpField) warpField).getType()) {

          case LABEL:
            dataPointHolder[i] = gts.getLabels().get(warpField.getKey());
            break;

          case ATTRIBUTE:
            dataPointHolder[i] = gts.getMetadata().getAttributes().get(warpField.getKey());
            break;
        }

      } else if (warpField instanceof TimestampWarpField) {
        dataPointHolder[i] = GTSHelper.tickAtIndex(gts, index);

      } else if (warpField instanceof LatitudeWarpField) {
        if (null == geoPointHolder) {
          dataPointHolder[i] = null;
        } else {
          dataPointHolder[i] = geoPointHolder[0];
        }

      } else if (warpField instanceof LongitudeWarpField) {
        if (null == geoPointHolder) {
          dataPointHolder[i] = null;
        } else {
          dataPointHolder[i] = geoPointHolder[1];
        }

      } else if (warpField instanceof ElevationWarpField) {
        long elevation = GTSHelper.elevationAtIndex(gts, index);

        if (GeoTimeSerie.NO_ELEVATION == elevation) {
          dataPointHolder[i] = null;
        } else {
          dataPointHolder[i] = elevation;
        }

      } else if (warpField instanceof ValueWarpField) {
        Object value = GTSHelper.valueAtIndex(gts, index);

        if (((ValueWarpField) warpField).getType().getCorrespondingClass() == value.getClass()) {
          dataPointHolder[i] = value;
        } else {
          dataPointHolder[i] = null;
        }
      } else {
        throw new WarpScriptException("Unrecognized field.");
      }
    }
  }

  /**
   * Populate dataPointHolder with data from input GtsDecoder in order given by the schema.
   *
   * @param index
   * @param gts
   */
  public void prepareGtsEncoderDataPoint(GTSDecoder decoder) throws WarpScriptException {

    double[] geoPointHolder = null;
    long location = decoder.getLocation();
    if (GeoTimeSerie.NO_LOCATION != location) {
      geoPointHolder = GeoXPLib.fromGeoXPPoint(location);
    }

    for (int i = 0; i < warpFields.size(); i++) {
      WarpField warpField = warpFields.get(i);

      if (warpField instanceof ClassnameWarpField) {
        dataPointHolder[i] = decoder.getName();

      } else if (warpField instanceof LabelWarpField) {

        switch (((LabelWarpField) warpField).getType()) {

          case LABEL:
            dataPointHolder[i] = decoder.getLabels().get(warpField.getKey());
            break;

          case ATTRIBUTE:
            dataPointHolder[i] = decoder.getMetadata().getAttributes().get(warpField.getKey());
            break;
        }

      } else if (warpField instanceof TimestampWarpField) {
        dataPointHolder[i] = decoder.getTimestamp();

      } else if (warpField instanceof LatitudeWarpField) {
        if (null == geoPointHolder) {
          dataPointHolder[i] = null;
        } else {
          dataPointHolder[i] = geoPointHolder[0];
        }

      } else if (warpField instanceof LongitudeWarpField) {
        if (null == geoPointHolder) {
          dataPointHolder[i] = null;
        } else {
          dataPointHolder[i] = geoPointHolder[1];
        }

      } else if (warpField instanceof ElevationWarpField) {
        long elevation = decoder.getElevation();

        if (GeoTimeSerie.NO_ELEVATION == elevation) {
          dataPointHolder[i] = null;
        } else {
          dataPointHolder[i] = elevation;
        }

      } else if (warpField instanceof ValueWarpField) {
        Object value = decoder.getValue();

        if (((ValueWarpField) warpField).getType().getCorrespondingClass() == value.getClass()) {
          dataPointHolder[i] = value;
        } else {
          dataPointHolder[i] = null;
        }

      } else {
        throw new WarpScriptException("Unrecognized field.");
      }
    }
  }

  public void writeListToStream(OutputStream out, List<Objects> list) throws WarpScriptException {

    try (ArrowStreamWriter writer =  new ArrowStreamWriter(root, dictionaryProvider, out)) {

      writer.start();
      for (Object o : list) {

        if (o instanceof GeoTimeSerie) {
          GeoTimeSerie gts = (GeoTimeSerie) o;

          for (int i = 0; i < gts.size(); i++) {
            prepareGtsDataPoint(i, gts);

            set(i, dataPointHolder);
          }

          root.setRowCount(gts.size());
          writer.writeBatch();

        } else if (o instanceof GTSEncoder) {
          GTSDecoder decoder = ((GTSEncoder) o).getDecoder(true);

          int i = 0;
          while (decoder.next()) {
            prepareGtsEncoderDataPoint(decoder);

            set(i++, dataPointHolder);
          }

          root.setRowCount(i);
          writer.writeBatch();

        } else {
          throw new WarpScriptException("Input list should contain only GTS or GTSENCODER.");
        }
      }
    } catch (IOException e) {
      throw new WarpScriptException(e);
    } finally {
      root.close();
    }
  }

  public void readStream(InputStream in) {

  }
}
