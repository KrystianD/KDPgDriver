using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace KDPgDriver.Utils
{
  public static class Helper
  {
    public class ModelExtractor
    {
      private object model;
    }

    public static string GetTableName(Type modelType)
    {
      var q = modelType.GetCustomAttributes(typeof(KDPgTableAttribute), false);

      if (q.Length == 0)
        throw new Exception("no table info");

      return ((KDPgTableAttribute) q[0]).Name;
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

    public static Type GetColumnType(MemberInfo memberInfo)
    {
      if (memberInfo is PropertyInfo p)
        return p.PropertyType;
      else
        throw new Exception("no");
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
  }
}