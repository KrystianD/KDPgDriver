using System.Reflection;
using KDPgDriver.Types;

namespace KDPgDriver.Utils
{
  public class KdPgColumnDescriptor
  {
    public string Name { get; }
    public KdPgTableDescriptor Table { get; internal set; }
    public KDPgColumnFlagsEnum Flags { get; }
    public KDPgValueType Type { get; }
    public PropertyInfo PropertyInfo { get; }

    public readonly TypedExpression TypedExpression;

    public KdPgColumnDescriptor(string name, KDPgColumnFlagsEnum flags, KDPgValueType type, PropertyInfo propertyInfo, KdPgTableDescriptor table)
    {
      Name = name;
      Flags = flags;
      Type = type;
      PropertyInfo = propertyInfo;
      Table = table;

      var rq = new RawQuery();
      rq.AppendColumn(this, new RawQuery.TableNamePlaceholder(table, table.Name));
      TypedExpression = new TypedExpression(rq, type);
    }
  }
}