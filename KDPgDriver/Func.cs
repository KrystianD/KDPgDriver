using System;
using KDPgDriver.Utils;

namespace KDPgDriver
{
  internal static class FuncInternal
  {
    public static TypedExpression MD5(TypedExpression query)
    {
      var rq = RawQuery.Create("MD5(").Append(query.RawQuery).Append(")");
      return new TypedExpression(rq, KDPgValueTypeInstances.String);
    }

    public static TypedExpression Count(TypedExpression query)
    {
      var rq = RawQuery.Create("COUNT(").Append(query.RawQuery).Append(")");
      return new TypedExpression(rq, KDPgValueTypeInstances.Integer64);
    }

    public static TypedExpression Now()
    {
      var rq = RawQuery.Create("NOW()");
      return new TypedExpression(rq, KDPgValueTypeInstances.Time);
    }
  }

  public static class Func
  {
    public static string MD5(string value)
    {
      throw new Exception("do not use directly");
    }

    public static long Count(object value)
    {
      throw new Exception("do not use directly");
    }

    public static DateTime Now()
    {
      throw new Exception("do not use directly");
    }

    public static T Raw<T>(string text)
    {
      throw new Exception("do not use directly");
    }
  }
}