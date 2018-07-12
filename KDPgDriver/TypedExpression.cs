using System;
using System.Collections;
using KDPgDriver.Builder;
using KDPgDriver.Utils;

namespace KDPgDriver {
  public class TypedExpression
  {
    public RawQuery RawQuery { get; }
    public KDPgValueType Type { get; }

    public TypedExpression(string rawQuery, KDPgValueType type)
    {
      RawQuery = RawQuery.Create(rawQuery);
      Type = type;
    }
    
    public TypedExpression(RawQuery rawQuery, KDPgValueType type)
    {
      RawQuery = rawQuery;
      Type = type;
    }

    public override string ToString()
    {
      return $"{RawQuery}, {Type}";
    }

    public static TypedExpression FromPgValue(Helper.PgValue value)
    {
      return new TypedExpression(RawQuery.Create(value), value.Type);
    }
  }
}