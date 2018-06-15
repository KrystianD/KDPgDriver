using System;
using System.ComponentModel;
using Npgsql.PostgresTypes;
using NpgsqlTypes;

namespace KDPgDriver
{
  public class KDPgTableAttribute : Attribute
  {
    public string Name { get; }

    public KDPgTableAttribute(string name)
    {
      Name = name;
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

  public enum KDPgColumnTypeEnum
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
  }

  public abstract class KDPgColumnType
  {
    public KDPgColumnTypeEnum BaseType;

    public abstract NpgsqlDbType NpgsqlType { get; }
    public abstract string PostgresType { get; }

    public KDPgColumnType(KDPgColumnTypeEnum baseType)
    {
      BaseType = baseType;
    }
  }

  public class KDPgColumnBooleanType : KDPgColumnType
  {
    public static KDPgColumnBooleanType Instance = new KDPgColumnBooleanType();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Boolean;
    public override string PostgresType => "bool";

    public KDPgColumnBooleanType() : base(KDPgColumnTypeEnum.Boolean) { }
  }

  public class KDPgColumnDateType : KDPgColumnType
  {
    public static KDPgColumnDateType Instance = new KDPgColumnDateType();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Date;
    public override string PostgresType => "date";

    public KDPgColumnDateType() : base(KDPgColumnTypeEnum.Date) { }
  }

  public class KDPgColumnTimeType : KDPgColumnType
  {
    public static KDPgColumnTimeType Instance = new KDPgColumnTimeType();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Time;
    public override string PostgresType => "time";

    public KDPgColumnTimeType() : base(KDPgColumnTypeEnum.Time) { }
  }

  public class KDPgColumnDateTimeType : KDPgColumnType
  {
    public static KDPgColumnDateTimeType Instance = new KDPgColumnDateTimeType();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Timestamp;
    public override string PostgresType => "timestamp";

    public KDPgColumnDateTimeType() : base(KDPgColumnTypeEnum.DateTime) { }
  }

  public class KDPgColumnIntegerType : KDPgColumnType
  {
    public static KDPgColumnIntegerType Instance = new KDPgColumnIntegerType();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Integer;
    public override string PostgresType => "int";

    public KDPgColumnIntegerType() : base(KDPgColumnTypeEnum.Integer) { }
  }

  public class KDPgColumnDecimal : KDPgColumnType
  {
    public static KDPgColumnDecimal Instance = new KDPgColumnDecimal();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Numeric;
    public override string PostgresType => "numeric";

    public KDPgColumnDecimal() : base(KDPgColumnTypeEnum.Decimal) { }
  }

  public class KDPgColumnStringType : KDPgColumnType
  {
    public static KDPgColumnStringType Instance = new KDPgColumnStringType();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Text;
    public override string PostgresType => "text";

    public KDPgColumnStringType() : base(KDPgColumnTypeEnum.String) { }
  }

  public class KDPgColumnUUIDType : KDPgColumnType
  {
    public static KDPgColumnUUIDType Instance = new KDPgColumnUUIDType();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Uuid;
    public override string PostgresType => "uuid";

    public KDPgColumnUUIDType() : base(KDPgColumnTypeEnum.UUID) { }
  }

  public class KDPgColumnArrayType : KDPgColumnType
  {
    public KDPgColumnType ItemType { get; }
    public Type ListType { get; }

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Array | ItemType.NpgsqlType;
    public override string PostgresType => $"{ItemType.PostgresType}[]";

    public KDPgColumnArrayType(KDPgColumnType itemType, Type listType) : base(KDPgColumnTypeEnum.Array)
    {
      ItemType = itemType;
      ListType = listType;
    }
  }

  public class KDPgColumnJsonType : KDPgColumnType
  {
    public static KDPgColumnJsonType Instance = new KDPgColumnJsonType();

    public override NpgsqlDbType NpgsqlType => NpgsqlDbType.Jsonb;
    public override string PostgresType => "jsonb";

    public Type BackingType { get; }

    public KDPgColumnJsonType(Type backingType = null) : base(KDPgColumnTypeEnum.Json)
    {
      BackingType = backingType;
    }
  }

  public class KDPgColumnTypeAttribute : Attribute
  {
    public KDPgColumnTypeEnum Type { get; }

    public KDPgColumnTypeAttribute(KDPgColumnTypeEnum type)
    {
      Type = type;
    }
  }
}