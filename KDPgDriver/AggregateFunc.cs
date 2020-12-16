using System;
using KDPgDriver.Utils;

// ReSharper disable UnusedMember.Global

namespace KDPgDriver
{
  internal static class AggregateFuncInternal
  {
    public static TypedExpression Max(TypedExpression query)
    {
      var rq = RawQuery.Create("MAX(").Append(query.RawQuery).Append(")");
      return new TypedExpression(rq, query.Type);
    }

    public static TypedExpression Min(TypedExpression query)
    {
      var rq = RawQuery.Create("MIN(").Append(query.RawQuery).Append(")");
      return new TypedExpression(rq, query.Type);
    }
  }

  public static class AggregateFunc
  {
    public static T Max<T>(T value)
    {
      throw new Exception("do not use directly");
    }

    public static T Min<T>(T value)
    {
      throw new Exception("do not use directly");
    }
  }
}