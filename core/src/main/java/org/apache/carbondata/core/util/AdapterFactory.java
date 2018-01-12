package org.apache.carbondata.core.util;

import java.lang.reflect.Type;

import org.apache.carbondata.core.metadata.datatype.ArrayType;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.metadata.datatype.MapType;
import org.apache.carbondata.core.metadata.datatype.StructType;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

public class AdapterFactory implements JsonDeserializer<DataType> {

  @Override public DataType deserialize(JsonElement jsonElement, Type type,
      JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
    JsonObject jsonObject = jsonElement.getAsJsonObject();
    DataType dataType;
    String typeName = jsonObject.get("name").getAsString();
    switch(typeName) {
      case "STRING":
        dataType = jsonDeserializationContext.deserialize(jsonElement, DataTypes.STRING.getClass());
        break;
      case "DECIMAL":
        dataType = jsonDeserializationContext.deserialize(jsonElement, DecimalType.class);
        break;
      case "DOUBLE":
        dataType = jsonDeserializationContext.deserialize(jsonElement, DataTypes.DOUBLE.getClass());
        break;
      case "TIMESTAMP":
        dataType = jsonDeserializationContext.deserialize(jsonElement, DataTypes.TIMESTAMP.getClass());
        break;
      case "DATE":
        dataType = jsonDeserializationContext.deserialize(jsonElement, DataTypes.DATE.getClass());
        break;
      case "INT":
        dataType = jsonDeserializationContext.deserialize(jsonElement, DataTypes.INT.getClass());
        break;
      case "FLOAT":
        dataType = jsonDeserializationContext.deserialize(jsonElement, DataTypes.FLOAT.getClass());
        break;
      case "LONG":
        dataType = jsonDeserializationContext.deserialize(jsonElement, DataTypes.LONG.getClass());
        break;
      case "SHORT":
        dataType = jsonDeserializationContext.deserialize(jsonElement, DataTypes.SHORT.getClass());
        break;
      case "BOOLEAN":
        dataType = jsonDeserializationContext.deserialize(jsonElement, DataTypes.BOOLEAN.getClass());
        break;
      case "BYTE":
        dataType = jsonDeserializationContext.deserialize(jsonElement, DataTypes.BYTE.getClass());
        break;
      case "ARRAY":
        dataType = jsonDeserializationContext.deserialize(jsonElement, ArrayType.class);
        break;
      case "STRUCT":
        dataType = jsonDeserializationContext.deserialize(jsonElement, StructType.class);
        break;
      case "MAP":
        dataType = jsonDeserializationContext.deserialize(jsonElement, MapType.class);
        break;
      case "SHORT_INT":
        dataType = jsonDeserializationContext.deserialize(jsonElement, DataTypes.SHORT_INT.getClass());
        break;
      case "BYTE_ARRAY":
        dataType = jsonDeserializationContext.deserialize(jsonElement, DataTypes.BYTE_ARRAY.getClass());
        break;
      case "LEGACY_LONG":
        dataType = jsonDeserializationContext.deserialize(jsonElement, DataTypes.LEGACY_LONG.getClass());
        break;
      default:
        throw new RuntimeException("create DataType with invalid name: " + typeName);
    }
    return dataType;
  }

}
