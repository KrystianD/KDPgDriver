using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;
using System.Reflection;
using System.Runtime.InteropServices.ComTypes;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
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
    public KDPgValueType Type { get; }
    public PropertyInfo PropertyInfo { get; }

    public KdPgColumnDescriptor(string name, KDPgColumnFlagsEnum flags, KDPgValueType type, PropertyInfo propertyInfo)
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

    public static KDPgValueType GetNpgsqlTypeFromObject(object obj) => GetNpgsqlTypeFromType(obj.GetType());

    public static KDPgValueType GetNpgsqlTypeFromType(Type type)
    {
      if (type.IsNullable())
        type = type.GetNullableInnerType();

      if (type == typeof(string))
        return KDPgValueTypeString.Instance;
      if (type == typeof(int))
        return KDPgValueTypeInteger.Instance;
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

      if (type == typeof(JToken) || type == typeof(JArray) || type == typeof(JObject))
        return KDPgValueTypeJson.Instance;

      if (type.IsGenericList() || type.IsGenericEumerable()) {
        var itemType = type.GetGenericArguments()[0];
        return new KDPgValueTypeArray(listType: type, itemType: GetNpgsqlTypeFromType(itemType));
      }

      if (type.IsArray) {
        var itemType = type.GetElementType();
        return new KDPgValueTypeArray(listType: type, itemType: GetNpgsqlTypeFromType(itemType));
      }

      if (TypeRegistry.HasEnumType(type)) {
        var entry = TypeRegistry.GetEnumEntryForType(type);
        return KDPgValueTypeEnum.GetInstance(entry.enumName, entry);
      }

      throw new Exception($"GetNpgsqlTypeFromObject: Type {type} not implemented");
    }

    public static string GetTableName(Type modelType)
    {
      var q = modelType.GetCustomAttributes(typeof(KDPgTableAttribute), false);

      if (q.Length == 0)
        throw new Exception("no table info");

      return ((KDPgTableAttribute) q[0]).Name;
    }

    public static string GetTableSchema(Type modelType)
    {
      var q = modelType.GetCustomAttributes(typeof(KDPgTableAttribute), false);

      if (q.Length == 0)
        throw new Exception("no table info");

      return ((KDPgTableAttribute) q[0]).Schema;
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

    public static object ConvertFromNpgsql(KDPgValueType type, object rawValue)
    {
      if (rawValue == null)
        return null;

      switch (type) {
        case KDPgValueTypeString _:
        case KDPgValueTypeInteger _:
        case KDPgValueTypeBoolean _:
        case KDPgValueTypeDateTime _:
        case KDPgValueTypeDate _:
        case KDPgValueTypeTime _:
        case KDPgValueTypeUUID _:
        case KDPgValueTypeDecimal _:
          return rawValue;

        case KDPgValueTypeArray arrayType:
          var rawItems = (IList) rawValue;
          var outputList = (IList) Activator.CreateInstance(arrayType.ListType);
          foreach (var rawItem in rawItems) {
            outputList.Add(ConvertFromNpgsql(arrayType.ItemType, rawItem));
          }

          return outputList;

        case KDPgValueTypeEnum enumType:
          return enumType.EnumEntry.nameToEnumFunc((string) rawValue);

        case KDPgValueTypeJson jsonType:
          if (jsonType.BackingType == null)
            return JToken.Parse((string) rawValue);
          else
            return JToken.Parse((string) rawValue).ToObject(jsonType.BackingType);

        default:
          throw new Exception($"ConvertFromNpgsql: Type {type} not implemented");
      }
    }

    public class PgValue
    {
      public object value;
      public NpgsqlDbType? NpgsqlType;
      public string PostgresType;

      public PgValue(object value, NpgsqlDbType? npgsqlType, string postgresType)
      {
        this.value = value;
        NpgsqlType = npgsqlType;
        PostgresType = postgresType;
      }

      public static PgValue Null = new PgValue(null, NpgsqlDbType.Unknown, null);
    }

    public static PgValue ConvertObjectToPgValue(object rawValue)
    {
      if (rawValue == null)
        return PgValue.Null;
      
      var npgValue = GetNpgsqlTypeFromObject(rawValue);
      var pgValue = ConvertToNpgsql(npgValue, rawValue);
      return pgValue;
    }

    public static PgValue ConvertToNpgsql(KdPgColumnDescriptor column, object rawValue)
    {
      return ConvertToNpgsql(column.PropertyInfo, rawValue);
    }

    public static PgValue ConvertToNpgsql(PropertyInfo columnProperty, object rawValue)
    {
      var column = GetColumnDataType(columnProperty);

      return ConvertToNpgsql(column.Type, rawValue);
    }

    public static PgValue ConvertToNpgsql(KDPgValueType type, object rawValue)
    {
      if (rawValue == null)
        return PgValue.Null;

      switch (type) {
        case KDPgValueTypeString _:
        case KDPgValueTypeInteger _:
        case KDPgValueTypeBoolean _:
        case KDPgValueTypeDateTime _:
        case KDPgValueTypeTime _:
        case KDPgValueTypeUUID _:
        case KDPgValueTypeDecimal _:
          return new PgValue(rawValue, type.NpgsqlType, type.PostgresType);

        case KDPgValueTypeEnum enumType:
          object v = enumType.EnumEntry.enumToNameFunc(rawValue);
          return new PgValue(v, type.NpgsqlType, type.PostgresType);

        case KDPgValueTypeDate _:
          return new PgValue(((DateTime) rawValue).Date, type.NpgsqlType, type.PostgresType);

        case KDPgValueTypeArray arrayType:
          IList objs = new List<string>();
          foreach (var rawItem in (IList) rawValue) {
            objs.Add(ConvertToNpgsql(arrayType.ItemType, rawItem).value);
          }

          return new PgValue(objs, type.NpgsqlType, type.PostgresType);

        case KDPgValueTypeJson jsonType:
          if (jsonType.BackingType == null)
            return new PgValue(((JToken) rawValue).ToString(Formatting.None), NpgsqlDbType.Jsonb, "jsonb");
          else
            return new PgValue(JsonConvert.SerializeObject(rawValue, Formatting.None), NpgsqlDbType.Jsonb, "jsonb");

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

        var table = new KdPgTableDescriptor(
            name: tableAttribute.Name,
            columns: tableType.GetProperties()
                              .Where(x => x.GetCustomAttribute<KDPgColumnAttribute>() != null)
                              .Select(CreateColumnDescriptor).ToList()
        );

        Console.WriteLine("ASD");
        foreach (var col in table.Columns) {
          PropertyInfoToColumnDesc[col.PropertyInfo] = col;
        }

        TypeToTableDesc[tableType] = table;

        TablesInitialized.Add(tableType);
      }
    }

    private static KDPgValueType CreateColumnDataType(PropertyInfo columnPropertyInfo)
    {
      Type propertyType = columnPropertyInfo.PropertyType;

      var columnTypeAttribute = columnPropertyInfo.GetCustomAttribute<KDPgColumnTypeAttribute>();
      if (columnTypeAttribute != null) {
        switch (columnTypeAttribute.TypeEnum) {
          case KDPgValueTypeKind.String:
            return KDPgValueTypeString.Instance;
          case KDPgValueTypeKind.Integer:
            return KDPgValueTypeInteger.Instance;

          case KDPgValueTypeKind.DateTime:
            return KDPgValueTypeDateTime.Instance;
          case KDPgValueTypeKind.Date:
            return KDPgValueTypeDate.Instance;
          case KDPgValueTypeKind.Time:
            return KDPgValueTypeTime.Instance;

          case KDPgValueTypeKind.Enum:
            var entry = TypeRegistry.GetEnumEntryForType(propertyType);
            return KDPgValueTypeEnum.GetInstance(entry.enumName, entry);

          case KDPgValueTypeKind.Json:
            return new KDPgValueTypeJson(propertyType);

          case KDPgValueTypeKind.Array:
            var listItemType = propertyType.GetGenericArguments()[0];
            return new KDPgValueTypeArray(listType: propertyType, itemType: GetNpgsqlTypeFromType(listItemType));

          default:
            throw new Exception($"CreateColumnDataType: Type {columnTypeAttribute.TypeEnum} not implemented");
        }
      }
      else {
        return GetNpgsqlTypeFromType(propertyType);
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

    public static string Quote(string str)
    {
      return "\"" + str + "\"";
    }

    public static string QuoteTable(string tableName, string schema = null)
    {
      return schema == null ? Quote(tableName) : $"{Quote(schema)}.{Quote(tableName)}";
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
  }
}