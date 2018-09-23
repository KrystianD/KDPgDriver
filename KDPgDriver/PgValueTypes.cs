using System;
using System.Collections;
using KDLib;
using KDPgDriver.Utils;
using Newtonsoft.Json.Linq;
using NpgsqlTypes;

namespace KDPgDriver
{
  public enum KDPgValueTypeKind
  {
    Null,
    Boolean,
    Date,
    Time,
    DateTime,
    Integer,
    Integer64,
    Decimal,
    String,
    UUID,
    Json,
    Binary,
    Enum,
    Array,
  }

  public abstract class KDPgValueType
  {
    public abstract NpgsqlDbType NpgsqlDbType { get; }

    public abstract Type CSharpType { get; }
    public abstract string PostgresTypeName { get; }

    public virtual Type PostgresPutType => CSharpType;
    public virtual string PostgresFetchType => PostgresTypeName; // enums are fetched as text
  }

  public static class KDPgValueTypeInstances
  {
    public static readonly KDPgValueTypeNull Null = new KDPgValueTypeNull();
    public static readonly KDPgValueTypeBoolean Boolean = new KDPgValueTypeBoolean();
    public static readonly KDPgValueTypeDate Date = new KDPgValueTypeDate();
    public static readonly KDPgValueTypeTime Time = new KDPgValueTypeTime();
    public static readonly KDPgValueTypeDateTime DateTime = new KDPgValueTypeDateTime();
    public static readonly KDPgValueTypeInteger Integer = new KDPgValueTypeInteger();
    public static readonly KDPgValueTypeInteger64 Integer64 = new KDPgValueTypeInteger64();
    public static readonly KDPgValueTypeDecimal Decimal = new KDPgValueTypeDecimal();
    public static readonly KDPgValueTypeString String = new KDPgValueTypeString();
    public static readonly KDPgValueTypeUUID UUID = new KDPgValueTypeUUID();
    public static readonly KDPgValueTypeJson Json = new KDPgValueTypeJson();
    public static readonly KDPgValueTypeBinary Binary = new KDPgValueTypeBinary();
  }

  public class KDPgValueTypeNull : KDPgValueType
  {
    public override Type CSharpType => throw new Exception("no type");
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Unknown;
    public override string PostgresTypeName => throw new Exception("no type");
  }

  public class KDPgValueTypeBoolean : KDPgValueType
  {
    public override Type CSharpType => typeof(bool);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Boolean;
    public override string PostgresTypeName => "bool";
  }

  public class KDPgValueTypeDate : KDPgValueType
  {
    public override Type CSharpType => typeof(DateTime);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Date;
    public override string PostgresTypeName => "date";
  }

  public class KDPgValueTypeTime : KDPgValueType
  {
    public override Type CSharpType => typeof(DateTime);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Time;
    public override string PostgresTypeName => "time";
  }

  public class KDPgValueTypeDateTime : KDPgValueType
  {
    public override Type CSharpType => typeof(TimeSpan);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Timestamp;
    public override string PostgresTypeName => "timestamp";
  }

  public class KDPgValueTypeInteger : KDPgValueType
  {
    public override Type CSharpType => typeof(int);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Integer;
    public override string PostgresTypeName => "int";
  }

  public class KDPgValueTypeInteger64 : KDPgValueType
  {
    public override Type CSharpType => typeof(long);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Bigint;
    public override string PostgresTypeName => "bigint";
  }

  public class KDPgValueTypeDecimal : KDPgValueType
  {
    public override Type CSharpType => typeof(decimal);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Numeric;
    public override string PostgresTypeName => "numeric";
  }

  public class KDPgValueTypeString : KDPgValueType
  {
    public override Type CSharpType => typeof(string);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Text;
    public override string PostgresTypeName => "text";
  }

  public class KDPgValueTypeUUID : KDPgValueType
  {
    public override Type CSharpType => typeof(Guid);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Uuid;
    public override string PostgresTypeName => "uuid";
  }

  public class KDPgValueTypeJson : KDPgValueType
  {
    public override Type CSharpType => typeof(JToken);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Jsonb;
    public override string PostgresTypeName => "jsonb";

    public Type BackingType { get; }

    public KDPgValueTypeJson(Type backingType = null)
    {
      BackingType = backingType;
    }
  }

  public class KDPgValueTypeBinary : KDPgValueType
  {
    public override Type CSharpType => typeof(byte[]);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Bytea;
    public override string PostgresTypeName => "bytea";
  }

  public class KDPgValueTypeEnum : KDPgValueType
  {
    public TypeRegistry.EnumEntry EnumEntry { get; }

    public override Type CSharpType => EnumEntry.Type;
    public override Type PostgresPutType => typeof(string);
    public override string PostgresFetchType => "text";

    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Text;
    public override string PostgresTypeName { get; }

    public KDPgValueTypeEnum(TypeRegistry.EnumEntry enumEntry)
    {
      EnumEntry = enumEntry;
      PostgresTypeName = Helper.QuoteTable(enumEntry.EnumName, enumEntry.Schema);
    }
  }

  public class KDPgValueTypeArray : KDPgValueType
  {
    public KDPgValueType ItemType { get; }
    // public Type ListType { get; }

    public override Type CSharpType { get; }

    // ReSharper disable once BitwiseOperatorOnEnumWithoutFlags
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Array | ItemType.NpgsqlDbType;
    public override string PostgresTypeName => $"{ItemType.PostgresTypeName}[]";
    public override string PostgresFetchType => $"{ItemType.PostgresFetchType}[]";

    public IList CreateToPgList()
    {
      return ReflectionUtils.CreateListInstance(ItemType.PostgresPutType);
    }

    public KDPgValueTypeArray(KDPgValueType itemType, Type nativeItemType /*, Type listType*/)
    {
      CSharpType = nativeItemType;
      ItemType = itemType;
      // ListType = listType;
    }
  }
}