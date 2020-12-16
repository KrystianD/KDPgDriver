using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Newtonsoft.Json;

namespace KDPgDriver.Utils
{
  public static class ModelsRegistry
  {
    private static readonly HashSet<Type> TablesInitialized = new HashSet<Type>();
    private static readonly Dictionary<Type, KdPgTableDescriptor> TypeToTableDesc = new Dictionary<Type, KdPgTableDescriptor>();
    private static readonly Dictionary<PropertyInfo, KdPgColumnDescriptor> PropertyInfoToColumnDesc = new Dictionary<PropertyInfo, KdPgColumnDescriptor>();

    // private static KDPgValueType CreatePgValueType(object obj) => CreatePgValueType(obj.GetType());

    public static bool IsJsonPropertyName(MemberInfo memberInfo)
    {
      var q = memberInfo.GetCustomAttributes(typeof(JsonPropertyAttribute), false);

      return q.Length > 0;
    }

    public static string GetJsonPropertyName(MemberInfo memberInfo)
    {
      var q = memberInfo.GetCustomAttributes(typeof(JsonPropertyAttribute), false);

      if (q.Length == 0)
        throw new Exception("no prop info");

      return ((JsonPropertyAttribute)q[0]).PropertyName;
    }

    public static string GetJsonPropertyNameOrNull(MemberInfo memberInfo)
    {
      var q = memberInfo.GetCustomAttributes(typeof(JsonPropertyAttribute), false);

      if (q.Length == 0)
        return null;

      return ((JsonPropertyAttribute)q[0]).PropertyName;
    }

    public static KDPgValueType GetJsonPropertyType(PropertyInfo memberInfo)
    {
      var q = memberInfo.GetCustomAttributes(typeof(JsonPropertyAttribute), false);

      if (q.Length == 0)
        throw new Exception("no prop info");

      var type = memberInfo.PropertyType;
      if (type == typeof(string))
        return KDPgValueTypeInstances.String;
      if (type == typeof(int))
        return KDPgValueTypeInstances.Integer;
      if (type == typeof(long))
        return KDPgValueTypeInstances.Integer64;
      if (type == typeof(bool))
        return KDPgValueTypeInstances.Boolean;

      return KDPgValueTypeInstances.Json;
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

        table.Columns = tableType.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                                 .Where(x => x.GetCustomAttribute<KDPgColumnAttribute>() != null)
                                 .Select(x => CreateColumnDescriptor(x, table)).ToList();

        foreach (var col in table.Columns) {
          PropertyInfoToColumnDesc[col.PropertyInfo] = col;
        }

        TypeToTableDesc[tableType] = table;

        TablesInitialized.Add(tableType);
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
          type: PgTypesConverter.CreatePgValueTypeFromProperty(columnPropertyInfo),
          propertyInfo: columnPropertyInfo,
          table: table);
    }
  }
}