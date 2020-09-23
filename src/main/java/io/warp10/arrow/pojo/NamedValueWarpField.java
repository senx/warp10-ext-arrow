package io.warp10.arrow.pojo;

import io.warp10.continuum.gts.GeoTimeSerie;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

public class NamedValueWarpField extends ValueWarpField {

  private final String name;
  private final Field field;

  public NamedValueWarpField(Type type, String name) {
    super(type);
    this.name = name;

    switch (type) {
      case LONG:
        field = Field.nullable(name,new ArrowType.Int(64, true));
        break;

      case DOUBLE:
        field =  Field.nullable(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        break;

      case BOOLEAN:
        field =  Field.nullable(name, new ArrowType.Bool());
        break;

      case STRING:
        field =  Field.nullable(name, new ArrowType.Utf8());
        break;

      case BYTES:
        field =  Field.nullable(name, new ArrowType.Binary());
        break;

      default:
        throw new RuntimeException("Unrecognized type.");
    }
  }

  public NamedValueWarpField(GeoTimeSerie.TYPE type, String name) {
    this(Type.valueOf(type.name()), name);
  }

  public NamedValueWarpField(BufferAllocator allocator, Type type, String name) {
    super(allocator, type);
    this.name = name;

    switch (type) {
      case LONG:
        field = Field.nullable(name,new ArrowType.Int(64, true));
        break;

      case DOUBLE:
        field =  Field.nullable(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        break;

      case BOOLEAN:
        field =  Field.nullable(name, new ArrowType.Bool());
        break;

      case STRING:
        field =  Field.nullable(name, new ArrowType.Utf8());
        break;

      case BYTES:
        field =  Field.nullable(name, new ArrowType.Binary());
        break;

      default:
        throw new RuntimeException("Unrecognized type.");
    }
  }

  public NamedValueWarpField(BufferAllocator allocator, GeoTimeSerie.TYPE type, String name) {
    this(allocator, Type.valueOf(type.name()), name);
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
