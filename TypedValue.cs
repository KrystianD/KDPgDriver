using System;

namespace KDPgDriver {
  public class TypedValue
  {
    public string Expression { get; }
    public Type Type { get; }

    public TypedValue(string expression, Type type)
    {
      Expression = expression;
      Type = type;
    }
  }
}