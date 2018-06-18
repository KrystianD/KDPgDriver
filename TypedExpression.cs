using System;

namespace KDPgDriver {
  public class TypedExpression
  {
    public string Expression { get; }
    public KDPgValueType Type { get; }

    public TypedExpression(string expression, KDPgValueType type)
    {
      Expression = expression;
      Type = type;
    }

    public override string ToString()
    {
      return $"{Expression}, {Type}";
    }
  }
}