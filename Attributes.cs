using System;
using System.Collections.Generic;
using System.ComponentModel;
using Npgsql.PostgresTypes;
using NpgsqlTypes;

namespace KDPgDriver
{
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

  [Flags]
  public enum KDPgColumnFlagsEnum
  {
    PrimaryKey = 1,
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

  public enum KDPgValueTypeKind
  {
    Boolean,
    String,
    Integer,
    Json,
    Array,
    DateTime,
    Date,
    Time,
    UUID,
    Decimal,
    Enum
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

  public abstract class KDPgValueType
  {
    // public KDPgValueTypeKind BaseType;

    public abstract NpgsqlDbType NpgsqlType { get; }
    public abstract string PostgresType { get; }

    public KDPgValueType(KDPgValueTypeKind baseType)
    {
      // BaseType = baseType;
    }
  }

  public class KDPgValueTypeBoolean : KDPgValueType
  {
    public static KDPgValueTypeBoolean Instance = new KDPgValueTypeBoolean();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Boolean;
    public override string PostgresType => "bool";

    public KDPgValueTypeBoolean() : base(KDPgValueTypeKind.Boolean) { }
  }

  public class KDPgValueTypeDate : KDPgValueType
  {
    public static KDPgValueTypeDate Instance = new KDPgValueTypeDate();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Date;
    public override string PostgresType => "date";

    public KDPgValueTypeDate() : base(KDPgValueTypeKind.Date) { }
  }

  public class KDPgValueTypeTime : KDPgValueType
  {
    public static KDPgValueTypeTime Instance = new KDPgValueTypeTime();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Time;
    public override string PostgresType => "time";

    public KDPgValueTypeTime() : base(KDPgValueTypeKind.Time) { }
  }

  public class KDPgValueTypeDateTime : KDPgValueType
  {
    public static KDPgValueTypeDateTime Instance = new KDPgValueTypeDateTime();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Timestamp;
    public override string PostgresType => "timestamp";

    public KDPgValueTypeDateTime() : base(KDPgValueTypeKind.DateTime) { }
  }

  public class KDPgValueTypeEnum : KDPgValueType
  {
    private readonly string _name;
    public TypeRegistry.EnumEntry EnumEntry { get; }

    private static Dictionary<string, KDPgValueTypeEnum> Instances = new Dictionary<string, KDPgValueTypeEnum>();

    public static KDPgValueTypeEnum GetInstance(string name, TypeRegistry.EnumEntry enumEntry)
    {
      if (Instances.ContainsKey(name))
        return Instances[name];
      else {
        var i = new KDPgValueTypeEnum(name, enumEntry);
        Instances[name] = i;
        return i;
      }
    }

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Text;
    public override string PostgresType => _name;

    public KDPgValueTypeEnum(string name, TypeRegistry.EnumEntry enumEntry) : base(KDPgValueTypeKind.Enum)
    {
      _name = name;
      EnumEntry = enumEntry;
    }
  }

  public class KDPgValueTypeInteger : KDPgValueType
  {
    public static KDPgValueTypeInteger Instance = new KDPgValueTypeInteger();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Integer;
    public override string PostgresType => "int";

    public KDPgValueTypeInteger() : base(KDPgValueTypeKind.Integer) { }
  }

  public class KDPgValueTypeDecimal : KDPgValueType
  {
    public static KDPgValueTypeDecimal Instance = new KDPgValueTypeDecimal();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Numeric;
    public override string PostgresType => "numeric";

    public KDPgValueTypeDecimal() : base(KDPgValueTypeKind.Decimal) { }
  }

  public class KDPgValueTypeString : KDPgValueType
  {
    public static KDPgValueTypeString Instance = new KDPgValueTypeString();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Text;
    public override string PostgresType => "text";

    public KDPgValueTypeString() : base(KDPgValueTypeKind.String) { }
  }

  public class KDPgValueTypeUUID : KDPgValueType
  {
    public static KDPgValueTypeUUID Instance = new KDPgValueTypeUUID();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Uuid;
    public override string PostgresType => "uuid";

    public KDPgValueTypeUUID() : base(KDPgValueTypeKind.UUID) { }
  }

  public class KDPgValueTypeArray : KDPgValueType
  {
    public KDPgValueType ItemType { get; }
    public Type ListType { get; }

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Array | ItemType.NpgsqlType;
    public override string PostgresType => $"{ItemType.PostgresType}[]";

    public KDPgValueTypeArray(KDPgValueType itemType, Type listType) : base(KDPgValueTypeKind.Array)
    {
      ItemType = itemType;
      ListType = listType;
    }
  }

  public class KDPgValueTypeJson : KDPgValueType
  {
    public static KDPgValueTypeJson Instance = new KDPgValueTypeJson();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Jsonb;
    public override string PostgresType => "jsonb";

    public Type BackingType { get; }

    public KDPgValueTypeJson(Type backingType = null) : base(KDPgValueTypeKind.Json)
    {
      BackingType = backingType;
    }
  }
}