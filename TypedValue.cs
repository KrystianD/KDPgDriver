using System;

namespace KDPgDriver {
  public class TypedValue
  {
    public string Expression { get; }
    public KDPgColumnType Type { get; }

    public TypedValue(string expression, KDPgColumnType type)
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