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
    Enum,
    Null,
  }

  public abstract class KDPgValueType
  {
    public abstract NpgsqlDbType NpgsqlDbType { get; }

    public abstract Type CSharpType { get; }
    public abstract string PostgresTypeName { get; }

    public virtual Type PostgresPutType => CSharpType;
    public virtual string PostgresFetchType => PostgresTypeName; // enums are fetched as text
  }

  public class KDPgValueTypeNull : KDPgValueType
  {
    public static readonly KDPgValueTypeNull Instance = new KDPgValueTypeNull();

    public override Type CSharpType => throw new Exception("no type");
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Unknown;
    public override string PostgresTypeName => throw new Exception("no type");
  }

  public class KDPgValueTypeBoolean : KDPgValueType
  {
    public static readonly KDPgValueTypeBoolean Instance = new KDPgValueTypeBoolean();

    public override Type CSharpType => typeof(bool);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Boolean;
    public override string PostgresTypeName => "bool";
  }

  public class KDPgValueTypeDate : KDPgValueType
  {
    public static readonly KDPgValueTypeDate Instance = new KDPgValueTypeDate();

    public override Type CSharpType => typeof(DateTime);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Date;
    public override string PostgresTypeName => "date";
  }

  public class KDPgValueTypeTime : KDPgValueType
  {
    public static readonly KDPgValueTypeTime Instance = new KDPgValueTypeTime();

    public override Type CSharpType => typeof(DateTime);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Time;
    public override string PostgresTypeName => "time";
  }

  public class KDPgValueTypeDateTime : KDPgValueType
  {
    public static readonly KDPgValueTypeDateTime Instance = new KDPgValueTypeDateTime();

    public override Type CSharpType => typeof(TimeSpan);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Timestamp;
    public override string PostgresTypeName => "timestamp";
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

  public class KDPgValueTypeInteger : KDPgValueType
  {
    public static readonly KDPgValueTypeInteger Instance = new KDPgValueTypeInteger();

    public override Type CSharpType => typeof(int);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Integer;
    public override string PostgresTypeName => "int";
  }

  public class KDPgValueTypeInteger64 : KDPgValueType
  {
    public static readonly KDPgValueTypeInteger64 Instance = new KDPgValueTypeInteger64();

    public override Type CSharpType => typeof(long);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Bigint;
    public override string PostgresTypeName => "bigint";
  }

  public class KDPgValueTypeDecimal : KDPgValueType
  {
    public static readonly KDPgValueTypeDecimal Instance = new KDPgValueTypeDecimal();

    public override Type CSharpType => typeof(decimal);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Numeric;
    public override string PostgresTypeName => "numeric";
  }

  public class KDPgValueTypeString : KDPgValueType
  {
    public static readonly KDPgValueTypeString Instance = new KDPgValueTypeString();

    public override Type CSharpType => typeof(string);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Text;
    public override string PostgresTypeName => "text";
  }

  public class KDPgValueTypeUUID : KDPgValueType
  {
    public static readonly KDPgValueTypeUUID Instance = new KDPgValueTypeUUID();

    public override Type CSharpType => typeof(Guid);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Uuid;
    public override string PostgresTypeName => "uuid";
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

    public KDPgValueTypeArray(KDPgValueType itemType, Type nativeItemType/*, Type listType*/)
    {
      CSharpType = nativeItemType;
      ItemType = itemType;
      // ListType = listType;
    }
  }

  public class KDPgValueTypeJson : KDPgValueType
  {
    public static readonly KDPgValueTypeJson Instance = new KDPgValueTypeJson();

    public override Type CSharpType => typeof(JToken);
    public override NpgsqlDbType NpgsqlDbType => NpgsqlDbType.Jsonb;
    public override string PostgresTypeName => "jsonb";

    public Type BackingType { get; }

    public KDPgValueTypeJson(Type backingType = null)
    {
      BackingType = backingType;
    }
  }
}