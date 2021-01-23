using System;
using KDPgDriver.Types;

// ReSharper disable InconsistentNaming

namespace KDPgDriver
{
  [Flags]
  public enum KDPgColumnFlagsEnum
  {
    PrimaryKey = 1,
    AutoIncrement = 2,
  }

  public class KDPgTableAttribute : Attribute
  {
    public string Name { get; }
    public string Schema { get; }

    public KDPgTableAttribute(string name, string schema = null)
    {
      Name = name;
      Schema = schema;
    }
  }

  public class KDPgColumnAttribute : Attribute
  {
    public string Name { get; }
    public KDPgColumnFlagsEnum Flags { get; }

    public KDPgColumnAttribute(string name, KDPgColumnFlagsEnum flags = 0)
    {
      Name = name;
      Flags = flags;
    }
  }

  public class KDPgColumnTypeAttribute : Attribute
  {
    public KDPgValueTypeKind TypeEnum { get; }

    public KDPgValueType Type { get; set; }

    public KDPgColumnTypeAttribute(KDPgValueTypeKind typeEnum)
    {
      TypeEnum = typeEnum;
    }
  }

  public class KDPgColumnArrayTypeAttribute : KDPgColumnTypeAttribute
  {
    public KDPgValueTypeKind ItemType { get; }

    public KDPgColumnArrayTypeAttribute(KDPgValueTypeKind itemType) : base(KDPgValueTypeKind.Array)
    {
      ItemType = itemType;
    }
  }
}