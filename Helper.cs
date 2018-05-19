using System;
using System.Reflection;

namespace KDPgDriver {
  public static class Helper
  {
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

    public static Type GetColumnType(MemberInfo memberInfo)
    {
      if (memberInfo is PropertyInfo p)
        return p.PropertyType;
      else
        throw new Exception("no");
    }
  }
}