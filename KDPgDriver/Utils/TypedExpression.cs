using KDPgDriver.Builders;
using KDPgDriver.Utils;

namespace KDPgDriver {
  public class TypedExpression
  {
    public RawQuery RawQuery { get; }
    public KDPgValueType Type { get; }

    public static TypedExpression Empty => new TypedExpression(RawQuery.Empty, null);

    public bool IsEmpty => RawQuery.IsEmpty;
    
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

    public static TypedExpression FromColumn(KdPgColumnDescriptor column)
    {
      return new TypedExpression(RawQuery.CreateColumnName(column.Name), column.Type);
    }
  }
}