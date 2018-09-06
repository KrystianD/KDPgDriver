using System;
using KDPgDriver.Utils;

namespace KDPgDriver
{
  internal static class FuncInternal
  {
    public static TypedExpression MD5(TypedExpression query)
    {
      var rq = RawQuery.Create("MD5(").Append(query.RawQuery).Append(")");
      return new TypedExpression(rq, KDPgValueTypeString.Instance);
    }
    
    public static TypedExpression Count(TypedExpression query)
    {
      var rq = RawQuery.Create("COUNT(").Append(query.RawQuery).Append(")");
      return new TypedExpression(rq, KDPgValueTypeInteger64.Instance);
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
  }
}