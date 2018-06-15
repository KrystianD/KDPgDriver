using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices.ComTypes;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Security.Cryptography.X509Certificates;
using KDLib;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using NpgsqlTypes;

namespace KDPgDriver.Utils
{
  public class KdPgTableDescriptor
  {
    public string Name { get; }
    public List<KdPgColumnDescriptor> Columns { get; }

    public KdPgColumnDescriptor PrimaryKey { get; }

    public KdPgTableDescriptor(string name, List<KdPgColumnDescriptor> columns)
    {
      Name = name;
      Columns = columns;

      PrimaryKey = columns.Find(x => (x.Flags & KDPgColumnFlagsEnum.PrimaryKey) > 0);
    }
  }

  public class KdPgColumnDescriptor
  {
    public string Name { get; }
    public KDPgColumnFlagsEnum Flags { get; }
    public KDPgColumnType Type { get; }
    public PropertyInfo PropertyInfo { get; }

    public KdPgColumnDescriptor(string name, KDPgColumnFlagsEnum flags, KDPgColumnType type, PropertyInfo propertyInfo)
    {
      Name = name;
      Flags = flags;
      Type = type;
      PropertyInfo = propertyInfo;
    }
  }

  public static class Helper
  {
    private static readonly HashSet<Type> TablesInitialized = new HashSet<Type>();
    private static readonly Dictionary<Type, KdPgTableDescriptor> TypeToTableDesc = new Dictionary<Type, KdPgTableDescriptor>();
    private static readonly Dictionary<PropertyInfo, KdPgColumnDescriptor> PropertyInfoToColumnDesc = new Dictionary<PropertyInfo, KdPgColumnDescriptor>();

    public static KDPgColumnType GetNpgsqlTypeFromObject(object obj) => GetNpgsqlTypeFromObject(obj.GetType());

    public static KDPgColumnType GetNpgsqlTypeFromObject(Type propertyType)
    {
      if (propertyType.IsNullable())
        propertyType = propertyType.GetNullableInnerType();

      if (propertyType == typeof(string))
        return KDPgColumnStringType.Instance;
      if (propertyType == typeof(int))
        return KDPgColumnIntegerType.Instance;
      if (propertyType == typeof(bool))
        return KDPgColumnBooleanType.Instance;
      if (propertyType == typeof(DateTime))
        return KDPgColumnDateTimeType.Instance;
      if (propertyType == typeof(TimeSpan))
        return KDPgColumnTimeType.Instance;
      if (propertyType == typeof(Guid))
        return KDPgColumnUUIDType.Instance;
      if (propertyType == typeof(decimal))
        return KDPgColumnDecimal.Instance;

      if (propertyType == typeof(JToken) || propertyType == typeof(JArray) || propertyType == typeof(JObject))
        return KDPgColumnJsonType.Instance;

      if (propertyType.IsGenericList()) {
        var itemType = propertyType.GetGenericArguments()[0];
        return new KDPgColumnArrayType(listType: propertyType, itemType: GetNpgsqlTypeFromObject(itemType));
      }

      if (propertyType.IsArray) {
        var itemType = propertyType.GetElementType();
        return new KDPgColumnArrayType(listType: propertyType, itemType: GetNpgsqlTypeFromObject(itemType));
      }

      throw new Exception($"GetNpgsqlTypeFromObject: Type {propertyType} not implemented");
    }

    public static string GetTableName(Type modelType)
    {
      var q = modelType.GetCustomAttributes(typeof(KDPgTableAttribute), false);

      if (q.Length == 0)
        throw new Exception("no table info");

      return ((KDPgTableAttribute) q[0]).Name;
    }

    public static string GetJsonPropertyName(MemberInfo memberInfo)
    {
      var q = memberInfo.GetCustomAttributes(typeof(JsonPropertyAttribute), false);

      if (q.Length == 0)
        throw new Exception("no prop info");

      return ((JsonPropertyAttribute) q[0]).PropertyName;
    }

    public static bool IsColumn(MemberInfo memberType)
    {
      if (!(memberType is PropertyInfo propertyInfo))
        return false;

      InitializeTable(memberType.DeclaringType);
      return PropertyInfoToColumnDesc.ContainsKey(propertyInfo);
    }

    public static KdPgColumnDescriptor GetColumn(MemberInfo memberType)
    {
      return GetColumn((PropertyInfo) memberType);
    }

    public static KdPgColumnDescriptor GetColumn(PropertyInfo memberType)
    {
      InitializeTable(memberType.DeclaringType);
      return PropertyInfoToColumnDesc[memberType];
    }

    // public static IList<PropertyInfo> GetModelColumns(Type modelType)
    // {
    //   return modelType.GetProperties().ToList();
    // }


    public static KdPgTableDescriptor GetTable(Type tableType)
    {
      InitializeTable(tableType);
      return TypeToTableDesc[tableType];
    }

    // public static IList<string> GetModelColumnNames(Type modelType)
    // {
    //   List<string> names = new List<string>();
    //   foreach (var propertyInfo in modelType.GetProperties()) {
    //     names.Add(GetColumn(propertyInfo));
    //   }
    //
    //   return names;
    // }

    public static object GetModelValueByColumn(object model, KdPgColumnDescriptor column)
    {
      return column.PropertyInfo.GetValue(model);
    }

    public static object GetModelValueByColumn(object model, PropertyInfo column)
    {
      return column.GetValue(model);
    }

    // public static Type GetColumnType(MemberInfo memberInfo)
    // {
    //   if (memberInfo is PropertyInfo p)
    //     return p.PropertyType;
    //   else
    //     throw new Exception("no");
    // }

    public static KdPgColumnDescriptor GetColumnDataType(PropertyInfo memberInfo)
    {
      InitializeTable(memberInfo.DeclaringType);
      return PropertyInfoToColumnDesc[memberInfo];
    }

    // public static bool IsSystemArray(object value)
    // {
    //   return value is Array;
    // }
    //
    // public static bool IsModelProperty(MemberInfo memberInfo)
    // {
    //   var q = memberInfo.GetCustomAttributes(typeof(KDPgColumnAttribute), false);
    //   return q.Length == 1;
    // }

    // public static object ConvertFromNpgsql(PropertyInfo columnProperty, object rawValue)
    // {
    //   KDPgColumnType type = GetColumnDataType(columnProperty);
    //   return ConvertFromNpgsql(type, rawValue);
    // }

    public static object ConvertFromNpgsql(KDPgColumnType type, object rawValue)
    {
      if (rawValue == null)
        return null;

      switch (type) {
        case KDPgColumnStringType _:
        case KDPgColumnIntegerType _:
        case KDPgColumnBooleanType _:
        case KDPgColumnDateTimeType _:
        case KDPgColumnDateType _:
        case KDPgColumnTimeType _:
        case KDPgColumnUUIDType _:
        case KDPgColumnDecimal _:
          return rawValue;

        case KDPgColumnArrayType arrayType:
          return Activator.CreateInstance(arrayType.ListType, rawValue);

        case KDPgColumnJsonType jsonType:
          if (jsonType.BackingType == null)
            return JToken.Parse((string) rawValue);
          else
            return JToken.Parse((string) rawValue).ToObject(jsonType.BackingType);

        default:
          throw new Exception($"ConvertFromNpgsql: Type {type} not implemented");
      }
    }

    public static Tuple<object, NpgsqlDbType> ConvertToNpgsql(KdPgColumnDescriptor column, object rawValue)
    {
      return ConvertToNpgsql(column.PropertyInfo, rawValue);
    }

    public static Tuple<object, NpgsqlDbType> ConvertToNpgsql(PropertyInfo columnProperty, object rawValue)
    {
      if (rawValue == null)
        return Tuple.Create<object, NpgsqlDbType>(null, NpgsqlDbType.Unknown);

      var column = GetColumnDataType(columnProperty);

      switch (column.Type) {
        case KDPgColumnStringType _:
        case KDPgColumnIntegerType _:
        case KDPgColumnArrayType _:
        case KDPgColumnBooleanType _:
        case KDPgColumnDateTimeType _:
        case KDPgColumnTimeType _:
        case KDPgColumnUUIDType _:
        case KDPgColumnDecimal _:
          return Tuple.Create(rawValue, column.Type.NpgsqlType);

        case KDPgColumnDateType _:
          return Tuple.Create((object) ((DateTime) rawValue).Date, column.Type.NpgsqlType);

        case KDPgColumnJsonType jsonType:
          if (jsonType.BackingType == null)
            return Tuple.Create((object) ((JToken) rawValue).ToString(Formatting.None), NpgsqlDbType.Jsonb);
          else
            return Tuple.Create((object) JsonConvert.SerializeObject(rawValue, Formatting.None), NpgsqlDbType.Jsonb);

        default:
          throw new Exception($"ConvertToNpgsql: Type {column} not implemented");
      }
    }

    // Initializers
    private static void InitializeTable(Type tableType)
    {
      if (TablesInitialized.Contains(tableType))
        return;

      var tableAttribute = tableType.GetCustomAttribute<KDPgTableAttribute>();

      var table = new KdPgTableDescriptor(
          name: tableAttribute.Name,
          columns: tableType.GetProperties()
                            .Where(x => x.GetCustomAttribute<KDPgColumnAttribute>() != null)
                            .Select(CreateColumnDescriptor).ToList()
      );

      foreach (var col in table.Columns) {
        PropertyInfoToColumnDesc[col.PropertyInfo] = col;
      }

      TypeToTableDesc[tableType] = table;

      TablesInitialized.Add(tableType);
    }

    private static KDPgColumnType CreateColumnDataType(PropertyInfo columnPropertyInfo)
    {
      Type propertyType = columnPropertyInfo.PropertyType;

      var columnTypeAttribute = columnPropertyInfo.GetCustomAttribute<KDPgColumnTypeAttribute>();
      if (columnTypeAttribute != null) {
        switch (columnTypeAttribute.Type) {
          case KDPgColumnTypeEnum.String:
            return KDPgColumnStringType.Instance;
          case KDPgColumnTypeEnum.Integer:
            return KDPgColumnIntegerType.Instance;

          case KDPgColumnTypeEnum.DateTime:
            return KDPgColumnDateTimeType.Instance;
          case KDPgColumnTypeEnum.Date:
            return KDPgColumnDateType.Instance;
          case KDPgColumnTypeEnum.Time:
            return KDPgColumnTimeType.Instance;

          case KDPgColumnTypeEnum.Json:
            return new KDPgColumnJsonType(propertyType);
          case KDPgColumnTypeEnum.Array:
            var itemType = propertyType.GetGenericArguments()[0];
            return new KDPgColumnArrayType(listType: propertyType, itemType: GetNpgsqlTypeFromObject(itemType));

          default:
            throw new Exception($"CreateColumnDataType: Type {columnTypeAttribute.Type} not implemented");
        }
      }
      else {
        return GetNpgsqlTypeFromObject(propertyType);
      }
    }

    private static KdPgColumnDescriptor CreateColumnDescriptor(PropertyInfo columnPropertyInfo)
    {
      var columnAttribute = columnPropertyInfo.GetCustomAttribute<KDPgColumnAttribute>();
      if (columnAttribute == null)
        throw new Exception("no column info");

      return new KdPgColumnDescriptor(
          name: columnAttribute.Name,
          flags: columnAttribute.Flags,
          type: CreateColumnDataType(columnPropertyInfo),
          propertyInfo: columnPropertyInfo);
    }
  }
}