using System;
using KDPgDriver.Builder;

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
  }
}