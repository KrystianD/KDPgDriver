using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using KDLib;
using KDPgDriver.Utils;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KDPgDriver.Types
{
  public static class PgTypesConverter
  {
    private static JsonSerializerSettings JsonSerializerSettings = new JsonSerializerSettings() {
        Converters = new List<JsonConverter>() {
            new DecimalJsonConverter(),
        },
        DateParseHandling = DateParseHandling.None,
    };

    public static KDPgValueType CreatePgValueTypeFromObjectType(Type type)
    {
      Type itemType;

      if (type.IsNullable())
        type = type.GetNullableInnerType();

      if (type == typeof(string) || type == typeof(char))
        return KDPgValueTypeInstances.String;
      if (type == typeof(int))
        return KDPgValueTypeInstances.Integer;
      if (type == typeof(long))
        return KDPgValueTypeInstances.Integer64;
      if (type == typeof(float))
        return KDPgValueTypeInstances.Float;
      if (type == typeof(double))
        return KDPgValueTypeInstances.Double;
      if (type == typeof(bool))
        return KDPgValueTypeInstances.Boolean;
      if (type == typeof(DateTime))
        return KDPgValueTypeInstances.DateTime;
      if (type == typeof(TimeSpan))
        return KDPgValueTypeInstances.Interval;
      if (type == typeof(Guid))
        return KDPgValueTypeInstances.UUID;
      if (type == typeof(decimal))
        return KDPgValueTypeInstances.Decimal;
      if (type == typeof(byte[]))
        return KDPgValueTypeInstances.Binary;

      if (type == typeof(JToken) || type == typeof(JArray) || type == typeof(JObject) || type == typeof(JValue))
        return KDPgValueTypeInstances.Json;

      if (Utils.Utils.CheckIfEnumerable(type, out itemType)) {
        return new KDPgValueTypeArray(
            itemType: CreatePgValueTypeFromObjectType(itemType),
            nativeItemType: itemType);
      }

      if (type.IsArray) {
        itemType = type.GetElementType();
        return new KDPgValueTypeArray(
            itemType: CreatePgValueTypeFromObjectType(itemType),
            nativeItemType: itemType);
      }

      if (TypeRegistry.HasEnumType(type))
        return TypeRegistry.GetEnumEntryForType(type).ValueType;

      throw new Exception($"GetNpgsqlTypeFromObject: Type {type} not implemented");
    }

    internal static KDPgValueType CreatePgValueTypeFromProperty(PropertyInfo columnPropertyInfo)
    {
      Type propertyType = columnPropertyInfo.PropertyType;

      var columnTypeAttribute = columnPropertyInfo.GetCustomAttribute<KDPgColumnTypeAttribute>();
      if (columnTypeAttribute != null) {
        switch (columnTypeAttribute.TypeEnum) {
          case KDPgValueTypeKind.String: return KDPgValueTypeInstances.String;
          case KDPgValueTypeKind.Integer: return KDPgValueTypeInstances.Integer;
          case KDPgValueTypeKind.Integer64: return KDPgValueTypeInstances.Integer64;
          case KDPgValueTypeKind.Float: return KDPgValueTypeInstances.Float;
          case KDPgValueTypeKind.Double: return KDPgValueTypeInstances.Double;
          case KDPgValueTypeKind.Boolean: return KDPgValueTypeInstances.Boolean;
          case KDPgValueTypeKind.UUID: return KDPgValueTypeInstances.UUID;
          case KDPgValueTypeKind.Decimal: return KDPgValueTypeInstances.Decimal;
          case KDPgValueTypeKind.Binary: return KDPgValueTypeInstances.Binary;
          case KDPgValueTypeKind.Null: return KDPgValueTypeInstances.Null;

          case KDPgValueTypeKind.Date: return KDPgValueTypeInstances.Date;
          case KDPgValueTypeKind.Time: return KDPgValueTypeInstances.Time;
          case KDPgValueTypeKind.DateTime: return KDPgValueTypeInstances.DateTime;
          case KDPgValueTypeKind.Interval: return KDPgValueTypeInstances.Interval;

          case KDPgValueTypeKind.Enum:
            var entry = TypeRegistry.GetEnumEntryForType(propertyType);
            return entry.ValueType;

          case KDPgValueTypeKind.Json:
            return new KDPgValueTypeJson(propertyType);

          case KDPgValueTypeKind.Array:
            var listItemType = propertyType.GetGenericArguments()[0];
            return new KDPgValueTypeArray( /*listType: propertyType,*/ nativeItemType: listItemType, itemType: CreatePgValueTypeFromObjectType(listItemType));

          default:
            throw new Exception($"CreateColumnDataType: Type {columnTypeAttribute.TypeEnum} not implemented");
        }
      }
      else {
        // try to infer property type
        return CreatePgValueTypeFromObjectType(propertyType);
      }
    }

    public static object ConvertFromRawSqlValue(KDPgValueType type, object rawSqlValue)
    {
      if (rawSqlValue == null)
        return null;

      switch (type) {
        case KDPgValueTypeString _:
        case KDPgValueTypeInteger _:
        case KDPgValueTypeInteger64 _:
        case KDPgValueTypeReal _:
        case KDPgValueTypeDoublePrecision _:
        case KDPgValueTypeBoolean _:
        case KDPgValueTypeDate _:
        case KDPgValueTypeTime _:
        case KDPgValueTypeDateTime _:
        case KDPgValueTypeInterval _:
        case KDPgValueTypeUUID _:
        case KDPgValueTypeDecimal _:
        case KDPgValueTypeBinary _:
          return rawSqlValue;

        case KDPgValueTypeArray arrayType:
          var rawItems = (IList)rawSqlValue;
          var outputList = ReflectionUtils.CreateListInstance(arrayType.CSharpItemType);
          foreach (var rawItem in rawItems)
            outputList.Add(ConvertFromRawSqlValue(arrayType.ItemType, rawItem));

          return outputList;

        case KDPgValueTypeEnum enumType:
          return enumType.EnumEntry.NameToEnumFunc((string)rawSqlValue);

        case KDPgValueTypeJson jsonType:
          if (jsonType.BackingType == null)
            return JToken.Parse((string)rawSqlValue);
          else
            return JsonConvert.DeserializeObject((string)rawSqlValue, jsonType.BackingType, JsonSerializerSettings);

        default:
          throw new Exception($"ConvertFromNpgsql: Type {type} not implemented");
      }
    }

    public static PgValue ConvertObjectToPgValue(object rawValue)
    {
      if (rawValue == null)
        return PgValue.Null;

      var pgValueType = CreatePgValueTypeFromObjectType(rawValue.GetType());
      var pgValue = ConvertToPgValue(pgValueType, rawValue);
      return pgValue;
    }

    public static PgValue ConvertToPgValue(KDPgValueType type, object rawValue)
    {
      if (rawValue == null)
        return PgValue.Null;

      switch (type) {
        case KDPgValueTypeString _:
        case KDPgValueTypeInteger _:
        case KDPgValueTypeInteger64 _:
        case KDPgValueTypeReal _:
        case KDPgValueTypeDoublePrecision _:
        case KDPgValueTypeBoolean _:
        case KDPgValueTypeTime _:
        case KDPgValueTypeDateTime _:
        case KDPgValueTypeInterval _:
        case KDPgValueTypeUUID _:
        case KDPgValueTypeDecimal _:
        case KDPgValueTypeBinary _:
          return new PgValue(rawValue, type);

        case KDPgValueTypeEnum enumType:
          string v = enumType.EnumEntry.EnumToNameFunc(rawValue);
          return new PgValue(v, type);

        case KDPgValueTypeDate _:
          return new PgValue(((DateTime)rawValue).Date, type);

        case KDPgValueTypeArray arrayType:
          var objs = arrayType.CreateToPgList();
          foreach (var rawItem in (IEnumerable)rawValue) {
            objs.Add(ConvertToPgValue(arrayType.ItemType, rawItem).Value);
          }

          return new PgValue(objs, type);

        case KDPgValueTypeJson jsonType:
          if (jsonType.BackingType == null)
            return new PgValue(((JToken)rawValue).ToString(Formatting.None), KDPgValueTypeInstances.Json);
          else
            return new PgValue(JsonConvert.SerializeObject(rawValue, Formatting.None, JsonSerializerSettings), KDPgValueTypeInstances.Json);

        default:
          throw new Exception($"ConvertToPgValue: Type {type} not implemented");
      }
    }

    public static string ConvertToPgString(object value)
    {
      return value switch {
          string s => "'" + s.Replace("'", "''") + "'",
          short v => v.ToString(),
          int v => v.ToString(),
          long v => v.ToString(),
          float v => v.ToString(CultureInfo.InvariantCulture),
          double v => v.ToString(CultureInfo.InvariantCulture),
          DateTime v => EscapeUtils.EscapePostgresString(v.ToString("yyyy-MM-dd HH:mm:ss.ffffff")),
          byte[] v => @$"E'\x{v.Select(x => x.ToString("x2")).JoinString()}'",
          IEnumerable v => @$"ARRAY[{v.Cast<object>().Select(ConvertToPgString).JoinString(",")}]",
          _ => throw new ArgumentException($"unable to escape value of type: {value.GetType()}"),
      };
    }
  }

  internal class DecimalJsonConverter : JsonConverter
  {
    public override bool CanConvert(Type objectType) => objectType == typeof(decimal) || objectType == typeof(decimal?);

    public override object ReadJson(
        JsonReader reader,
        Type objectType,
        object existingValue,
        JsonSerializer serializer)
    {
      if (reader.TokenType != JsonToken.String)
        throw new JsonSerializationException($"Unexpected token when parsing decimal. Expected String, got {reader.TokenType}.");

      return decimal.Parse((string)reader.Value);
    }

    public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
    {
      writer.WriteValue(((decimal)value).ToString((IFormatProvider)CultureInfo.InvariantCulture));
    }
  }
}