using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices.ComTypes;
using KDLib;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using NpgsqlTypes;

namespace KDPgDriver.Utils
{
  public static class Helper
  {
    private static readonly HashSet<Type> TablesInitialized = new HashSet<Type>();
    private static readonly Dictionary<PropertyInfo, KDPgColumnType> Types = new Dictionary<PropertyInfo, KDPgColumnType>();

    private static void InitializeTable(Type tableType)
    {
      if (TablesInitialized.Contains(tableType))
        return;

      foreach (var columnPropertyInfo in GetModelColumns(tableType))
        Types[columnPropertyInfo] = CreateColumnDataType(columnPropertyInfo);

      TablesInitialized.Add(tableType);
    }

    private static KDPgColumnType CreateColumnDataType(PropertyInfo columnPropertyInfo)
    {
      var q = columnPropertyInfo.GetCustomAttributes(typeof(KDPgColumnAttribute), false);
      if (q.Length == 0)
        throw new Exception("no column info");

      Type propertyType = columnPropertyInfo.PropertyType;
      KDPgColumnAttribute columnAttribute = (KDPgColumnAttribute) q[0];

      var q2 = columnPropertyInfo.GetCustomAttributes(typeof(KDPgColumnTypeAttribute), false);
      if (q2.Length > 0) {
        KDPgColumnTypeAttribute columnTypeAttribute = (KDPgColumnTypeAttribute) q2[0];

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
      var q = memberType.GetCustomAttributes(typeof(KDPgColumnAttribute), false);
      return q.Length > 0;
    }

    public static string GetColumnName(MemberInfo memberType)
    {
      var q = memberType.GetCustomAttributes(typeof(KDPgColumnAttribute), false);

      if (q.Length == 0)
        throw new Exception("no column info");

      return ((KDPgColumnAttribute) q[0]).Name;
    }

    public static IList<PropertyInfo> GetModelColumns(Type modelType)
    {
      return modelType.GetProperties().ToList();
    }

    public static IList<string> GetModelColumnNames(Type modelType)
    {
      List<string> names = new List<string>();
      foreach (var propertyInfo in modelType.GetProperties()) {
        names.Add(GetColumnName(propertyInfo));
      }

      return names;
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

    public static KDPgColumnType GetColumnDataType(PropertyInfo memberInfo)
    {
      InitializeTable(memberInfo.DeclaringType);
      return Types[memberInfo];
    }

    public static bool IsSystemArray(object value)
    {
      return value is Array;
    }

    public static bool IsModelProperty(MemberInfo memberInfo)
    {
      var q = memberInfo.GetCustomAttributes(typeof(KDPgColumnAttribute), false);
      return q.Length == 1;
    }

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

    public static Tuple<object, NpgsqlDbType> ConvertToNpgsql(PropertyInfo columnProperty, object rawValue)
    {
      if (rawValue == null)
        return Tuple.Create<object, NpgsqlDbType>(null, NpgsqlDbType.Unknown);

      var type = GetColumnDataType(columnProperty);

      switch (type) {
        case KDPgColumnStringType _:
        case KDPgColumnIntegerType _:
        case KDPgColumnArrayType _:
        case KDPgColumnBooleanType _:
        case KDPgColumnDateTimeType _:
        case KDPgColumnTimeType _:
          return Tuple.Create(rawValue, type.NpgsqlType);

        case KDPgColumnDateType _:
          return Tuple.Create((object) ((DateTime) rawValue).Date, type.NpgsqlType);

        case KDPgColumnJsonType jsonType:
          if (jsonType.BackingType == null)
            return Tuple.Create((object) ((JToken) rawValue).ToString(Formatting.None), NpgsqlDbType.Jsonb);
          else
            return Tuple.Create((object) JsonConvert.SerializeObject(rawValue, Formatting.None), NpgsqlDbType.Jsonb);

        default:
          throw new Exception($"ConvertToNpgsql: Type {type} not implemented");
      }
    }
  }
}