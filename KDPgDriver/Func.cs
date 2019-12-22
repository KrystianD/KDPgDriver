using System;
using KDPgDriver.Builders;
using KDPgDriver.Utils;

// ReSharper disable UnusedMember.Global

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

    public static TypedExpression Coalesce(TypedExpression value1,
                                           TypedExpression value2 = null,
                                           TypedExpression value3 = null,
                                           TypedExpression value4 = null,
                                           TypedExpression value5 = null)
    {
      var rq = RawQuery.Create("COALESCE(");
      rq.Append(value1.RawQuery);

      if (value2 != null) {
        rq.Append(", ");
        rq.Append(value2.RawQuery);
      }

      if (value3 != null) {
        rq.Append(", ");
        rq.Append(value3.RawQuery);
      }

      if (value4 != null) {
        rq.Append(", ");
        rq.Append(value4.RawQuery);
      }

      if (value5 != null) {
        rq.Append(", ");
        rq.Append(value5.RawQuery);
      }

      rq.Append(")");

      return new TypedExpression(rq, value1.Type);
    }

    public static TypedExpression GetVariableInt(string name)
    {
      return ExpressionBuilders.GetConfigInt(name);
    }

    public static TypedExpression GetVariableText(string name)
    {
      return ExpressionBuilders.GetConfigText(name);
    }

    public static TypedExpression Date(TypedExpression query)
    {
      var rq = RawQuery.Create("DATE(").Append(query.RawQuery).Append(")");
      return new TypedExpression(rq, KDPgValueTypeInstances.Date);
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

    public static T Coalesce<T>(T value1)
    {
      throw new Exception("do not use directly");
    }

    public static T Coalesce<T>(T value1, T value2)
    {
      throw new Exception("do not use directly");
    }

    public static T Coalesce<T>(T value1, T value2, T value3)
    {
      throw new Exception("do not use directly");
    }

    public static T Coalesce<T>(T value1, T value2, T value3, T value4)
    {
      throw new Exception("do not use directly");
    }

    public static T Coalesce<T>(T value1, T value2, T value3, T value4, T value5)
    {
      throw new Exception("do not use directly");
    }

    public static int GetVariableInt(string name)
    {
      throw new Exception("do not use directly");
    }

    public static string GetVariableText(string name)
    {
      throw new Exception("do not use directly");
    }

    public static DateTime Date(DateTime date)
    {
      throw new Exception("do not use directly");
    }
  }
}