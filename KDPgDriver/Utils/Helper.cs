using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using KDLib;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KDPgDriver.Utils
{
  public static class Helper
  {
    private static bool CheckIfEnumerable(Type type, out Type itemType)
    {
      itemType = null;

      foreach (var i in type.GetInterfaces()) {
        var isEnumerable = i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>);
        if (isEnumerable) {
          itemType = i.GetGenericArguments()[0];
          return true;
        }
      }

      return false;
    }

    private static readonly HashSet<Type> TablesInitialized = new HashSet<Type>();
    private static readonly Dictionary<Type, KdPgTableDescriptor> TypeToTableDesc = new Dictionary<Type, KdPgTableDescriptor>();
    private static readonly Dictionary<PropertyInfo, KdPgColumnDescriptor> PropertyInfoToColumnDesc = new Dictionary<PropertyInfo, KdPgColumnDescriptor>();

    // private static KDPgValueType CreatePgValueType(object obj) => CreatePgValueType(obj.GetType());

    private static KDPgValueType CreatePgValueTypeFromObjectType(Type type)
    {
      Type itemType;

      if (type.IsNullable())
        type = type.GetNullableInnerType();

      if (type == typeof(string) || type == typeof(char))
        return KDPgValueTypeString.Instance;
      if (type == typeof(int))
        return KDPgValueTypeInteger.Instance;
      if (type == typeof(long))
        return KDPgValueTypeInteger64.Instance;
      if (type == typeof(bool))
        return KDPgValueTypeBoolean.Instance;
      if (type == typeof(DateTime))
        return KDPgValueTypeDateTime.Instance;
      if (type == typeof(TimeSpan))
        return KDPgValueTypeTime.Instance;
      if (type == typeof(Guid))
        return KDPgValueTypeUUID.Instance;
      if (type == typeof(decimal))
        return KDPgValueTypeDecimal.Instance;

      if (type == typeof(JToken) || type == typeof(JArray) || type == typeof(JObject) || type == typeof(JValue))
        return KDPgValueTypeJson.Instance;

      if (CheckIfEnumerable(type, out itemType)) {
        return new KDPgValueTypeArray(
            // listType: type,
            itemType: CreatePgValueTypeFromObjectType(itemType),
            nativeItemType: itemType);
      }

      if (type.IsArray) {
        itemType = type.GetElementType();
        return new KDPgValueTypeArray(
            // listType: type,
            itemType: CreatePgValueTypeFromObjectType(itemType),
            nativeItemType: itemType);
      }

      if (TypeRegistry.HasEnumType(type))
        return TypeRegistry.GetEnumEntryForType(type).ValueType;

      throw new Exception($"GetNpgsqlTypeFromObject: Type {type} not implemented");
    }

    public static string GetJsonPropertyName(MemberInfo memberInfo)
    {
      var q = memberInfo.GetCustomAttributes(typeof(JsonPropertyAttribute), false);

      if (q.Length == 0)
        throw new Exception("no prop info");

      return ((JsonPropertyAttribute) q[0]).PropertyName;
    }

    public static string GetJsonPropertyNameOrNull(MemberInfo memberInfo)
    {
      var q = memberInfo.GetCustomAttributes(typeof(JsonPropertyAttribute), false);

      if (q.Length == 0)
        return null;

      return ((JsonPropertyAttribute) q[0]).PropertyName;
    }

    public static KDPgValueType GetJsonPropertyType(PropertyInfo memberInfo)
    {
      var q = memberInfo.GetCustomAttributes(typeof(JsonPropertyAttribute), false);

      if (q.Length == 0)
        throw new Exception("no prop info");

      var type = memberInfo.PropertyType;
      if (type == typeof(string))
        return KDPgValueTypeString.Instance;
      if (type == typeof(int))
        return KDPgValueTypeInteger.Instance;
      if (type == typeof(bool))
        return KDPgValueTypeBoolean.Instance;

      return KDPgValueTypeJson.Instance;
    }

    public static bool IsColumn(MemberInfo memberType)
    {
      if (!(memberType is PropertyInfo propertyInfo))
        return false;

      InitializeTable(memberType.DeclaringType);
      return PropertyInfoToColumnDesc.ContainsKey(propertyInfo);
    }

    public static bool IsTable(Type type)
    {
      InitializeTable(type);
      return TypeToTableDesc.ContainsKey(type);
    }

    // public static KdPgColumnDescriptor GetColumn(MemberInfo memberType)
    // {
    //   return GetColumn((PropertyInfo) memberType);
    // }

    public static KdPgColumnDescriptor GetColumn(PropertyInfo memberType)
    {
      InitializeTable(memberType.DeclaringType);
      return PropertyInfoToColumnDesc[memberType];
    }

    public static KdPgTableDescriptor GetTable<TModel>()
    {
      var tableType = typeof(TModel);
      InitializeTable(tableType);
      return TypeToTableDesc[tableType];
    }

    public static KdPgTableDescriptor GetTable(Type tableType)
    {
      InitializeTable(tableType);
      return TypeToTableDesc[tableType];
    }

    public static object GetModelValueByColumn(object model, KdPgColumnDescriptor column)
    {
      return column.PropertyInfo.GetValue(model);
    }

    // public static object GetModelValueByColumn(object model, PropertyInfo column)
    // {
    //   return column.GetValue(model);
    // }

    public static object ConvertFromRawSqlValue(KDPgValueType type, object rawSqlValue)
    {
      if (rawSqlValue == null)
        return null;

      switch (type) {
        case KDPgValueTypeString _:
        case KDPgValueTypeInteger _:
        case KDPgValueTypeInteger64 _:
        case KDPgValueTypeBoolean _:
        case KDPgValueTypeDateTime _:
        case KDPgValueTypeDate _:
        case KDPgValueTypeTime _:
        case KDPgValueTypeUUID _:
        case KDPgValueTypeDecimal _:
          return rawSqlValue;

        case KDPgValueTypeArray arrayType:
          var rawItems = (IList) rawSqlValue;
          var outputList = ReflectionUtils.CreateListInstance(arrayType.CSharpType);
          foreach (var rawItem in rawItems)
            outputList.Add(ConvertFromRawSqlValue(arrayType.ItemType, rawItem));

          return outputList;

        case KDPgValueTypeEnum enumType:
          return enumType.EnumEntry.NameToEnumFunc((string) rawSqlValue);

        case KDPgValueTypeJson jsonType:
          if (jsonType.BackingType == null)
            return JToken.Parse((string) rawSqlValue);
          else
            return JToken.Parse((string) rawSqlValue).ToObject(jsonType.BackingType);

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
        case KDPgValueTypeBoolean _:
        case KDPgValueTypeDateTime _:
        case KDPgValueTypeTime _:
        case KDPgValueTypeUUID _:
        case KDPgValueTypeDecimal _:
          return new PgValue(rawValue, type);

        case KDPgValueTypeEnum enumType:
          object v = enumType.EnumEntry.EnumToNameFunc(rawValue);
          return new PgValue(v, type);

        case KDPgValueTypeDate _:
          return new PgValue(((DateTime) rawValue).Date, type);

        case KDPgValueTypeArray arrayType:
          var objs = arrayType.CreateToPgList();
          foreach (var rawItem in (IEnumerable) rawValue) {
            objs.Add(ConvertToPgValue(arrayType.ItemType, rawItem).Value);
          }

          return new PgValue(objs, type);

        case KDPgValueTypeJson jsonType:
          if (jsonType.BackingType == null)
            return new PgValue(((JToken) rawValue).ToString(Formatting.None), KDPgValueTypeJson.Instance);
          else
            return new PgValue(JsonConvert.SerializeObject(rawValue, Formatting.None), KDPgValueTypeJson.Instance);

        default:
          throw new Exception($"ConvertToNpgsql: Type {type} not implemented");
      }
    }

    // Initializers
    private static void InitializeTable(Type tableType)
    {
      lock (PropertyInfoToColumnDesc) {
        if (TablesInitialized.Contains(tableType))
          return;

        var tableAttribute = tableType.GetCustomAttribute<KDPgTableAttribute>();

        if (tableAttribute == null)
          return;

        var table = new KdPgTableDescriptor(
            modelType: tableType,
            name: tableAttribute.Name,
            schema: tableAttribute.Schema);

        table.Columns = tableType.GetProperties()
                                 .Where(x => x.GetCustomAttribute<KDPgColumnAttribute>() != null)
                                 .Select(x => CreateColumnDescriptor(x, table)).ToList();

        foreach (var col in table.Columns) {
          PropertyInfoToColumnDesc[col.PropertyInfo] = col;
        }

        TypeToTableDesc[tableType] = table;

        TablesInitialized.Add(tableType);
      }
    }

    private static KDPgValueType CreatePgValueTypeFromProperty(PropertyInfo columnPropertyInfo)
    {
      Type propertyType = columnPropertyInfo.PropertyType;

      var columnTypeAttribute = columnPropertyInfo.GetCustomAttribute<KDPgColumnTypeAttribute>();
      if (columnTypeAttribute != null) {
        switch (columnTypeAttribute.TypeEnum) {
          case KDPgValueTypeKind.String: return KDPgValueTypeString.Instance;
          case KDPgValueTypeKind.Integer: return KDPgValueTypeInteger.Instance;
          case KDPgValueTypeKind.Boolean: return KDPgValueTypeBoolean.Instance;
          case KDPgValueTypeKind.UUID: return KDPgValueTypeUUID.Instance;
          case KDPgValueTypeKind.Decimal: return KDPgValueTypeDecimal.Instance;
          case KDPgValueTypeKind.Null: return KDPgValueTypeNull.Instance;

          case KDPgValueTypeKind.DateTime: return KDPgValueTypeDateTime.Instance;
          case KDPgValueTypeKind.Date: return KDPgValueTypeDate.Instance;
          case KDPgValueTypeKind.Time: return KDPgValueTypeTime.Instance;

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

    private static KdPgColumnDescriptor CreateColumnDescriptor(PropertyInfo columnPropertyInfo, KdPgTableDescriptor table)
    {
      var columnAttribute = columnPropertyInfo.GetCustomAttribute<KDPgColumnAttribute>();
      if (columnAttribute == null)
        throw new Exception("no column info");

      return new KdPgColumnDescriptor(
          name: columnAttribute.Name,
          flags: columnAttribute.Flags,
          type: CreatePgValueTypeFromProperty(columnPropertyInfo),
          propertyInfo: columnPropertyInfo,
          table: table);
    }

    // Helpers
    private static readonly HashSet<char> ValidObjectNameChars = "abcdefghijklmnoprstuwxvyz0123456789_".ToHashSet();

    public static string QuoteObjectName(string str)
    {
      if (str.ToHashSet().IsSubsetOf(ValidObjectNameChars))
        return str;
      else
        return "\"" + str + "\"";
    }

    public static string QuoteTable(string tableName, string schema = null)
    {
      return schema == null ? QuoteObjectName(tableName) : $"{QuoteObjectName(schema)}.{QuoteObjectName(tableName)}";
    }

    public static string EscapePostgresValue(object value)
    {
      switch (value) {
        case string s:
          return "'" + s.Replace("'", "''") + "'";
        case int v:
          return v.ToString();
        default:
          throw new Exception($"unable to escape value of type: {value.GetType()}");
      }
    }

    internal static IsolationLevel ToIsolationLevel(KDPgIsolationLevel level)
    {
      switch (level) {
        case KDPgIsolationLevel.ReadCommitted: return IsolationLevel.ReadCommitted;
        case KDPgIsolationLevel.RepeatableRead: return IsolationLevel.RepeatableRead;
        case KDPgIsolationLevel.Serializable: return IsolationLevel.Serializable;
        default: throw new ArgumentOutOfRangeException(nameof(level), level, null);
      }
    }
  }
}