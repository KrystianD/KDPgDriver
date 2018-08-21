using System;
using System.Collections.Generic;

namespace KDPgDriver.Utils
{
  public class KdPgTableDescriptor
  {
    private List<KdPgColumnDescriptor> _columns;
    public string Name { get; }
    public string Schema { get; }
    public Type ModelType { get; }

    public List<KdPgColumnDescriptor> Columns
    {
      get => _columns;
      set
      {
        _columns = value;
        PrimaryKey = _columns.Find(x => (x.Flags & KDPgColumnFlagsEnum.PrimaryKey) > 0);
      }
    }

    // public readonly TypedExpression TypedExpression;

    public KdPgColumnDescriptor PrimaryKey { get; private set; }

    public KdPgTableDescriptor(Type modelType, string name, string schema)
    {
      ModelType = modelType;
      Name = name;
      Schema = schema;
      // Columns = columns;

      // TypedExpression = new TypedExpression(RawQuery.CreateTable(this), null);
    }
  }
}