package io.warp10.arrow.pojo;

import io.warp10.continuum.gts.GeoTimeSerie;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Map;

public class NamedValueWarpField extends ValueWarpField {

  private final String name;
  private final Field field;

  public NamedValueWarpField(Type type, String name) {
    this(type, name, null);
  }

  public NamedValueWarpField(Type type, String name, Map<String, String> metadata) {
    super(type);
    this.name = name;
    ArrowType arrowType;

    switch (type) {
      case LONG:
        arrowType = new ArrowType.Int(64, true);
        field = new Field(name, new FieldType(true, arrowType, null, metadata),null);
        break;

      case DOUBLE:
        arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        field = new Field(name, new FieldType(true, arrowType, null, metadata),null);
        break;

      case BOOLEAN:
        arrowType = new ArrowType.Bool();
        field = new Field(name, new FieldType(true, arrowType, null, metadata),null);
        break;

      case STRING:
        arrowType = new ArrowType.Utf8();
        field = new Field(name, new FieldType(true, arrowType, null, metadata),null);
        break;

      case BYTES:
        arrowType = new ArrowType.Binary();
        field = new Field(name, new FieldType(true, arrowType, null, metadata),null);
        break;

      default:
        throw new RuntimeException("Unrecognized type.");
    }
  }

  public NamedValueWarpField(GeoTimeSerie.TYPE type, String name, Map<String, String> metadata) {
    this(Type.valueOf(type.name()), name, metadata);
  }

  public NamedValueWarpField(BufferAllocator allocator, Type type, String name) {
    this(allocator, type, name, null);
  }

  public NamedValueWarpField(BufferAllocator allocator, Type type, String name, Map<String, String> metadata) {
    super(allocator, type);
    this.name = name;
    ArrowType arrowType;

    switch (type) {
      case LONG:
        arrowType = new ArrowType.Int(64, true);
        field = new Field(name, new FieldType(true, arrowType, null, metadata),null);
        break;

      case DOUBLE:
        arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        field = new Field(name, new FieldType(true, arrowType, null, metadata),null);
        break;

      case BOOLEAN:
        arrowType = new ArrowType.Bool();
        field = new Field(name, new FieldType(true, arrowType, null, metadata),null);
        break;

      case STRING:
        arrowType = new ArrowType.Utf8();
        field = new Field(name, new FieldType(true, arrowType, null, metadata),null);
        break;

      case BYTES:
        arrowType = new ArrowType.Binary();
        field = new Field(name, new FieldType(true, arrowType, null, metadata),null);
        break;

      default:
        throw new RuntimeException("Unrecognized type.");
    }
  }

  public NamedValueWarpField(BufferAllocator allocator, GeoTimeSerie.TYPE type, String name, Map<String, String> metadata) {
    this(allocator, Type.valueOf(type.name()), name, metadata);
  }

  @Override
  public String getKey() {
    return name;
  }

  @Override
  public Field getField() {
    return field;
  }
}
