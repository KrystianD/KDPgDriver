using System;
using System.Collections.Generic;

namespace KDPgDriver
{
  public static class TypeRegistry
  {
    public class EnumEntry
    {
      public Type Type { get; internal set; }
      public string EnumName { get; internal set; }
      public Func<object, string> EnumToNameFunc { get; internal set; }
      public Func<string, object> NameToEnumFunc { get; internal set; }
      public string Schema { get; internal set; }

      public KDPgValueTypeEnum ValueType { get; internal set; }
    }

    private static readonly List<EnumEntry> Entries = new List<EnumEntry>();

    public static void RegisterEnum<T>(string enumName, Func<T, string> enumToNameFunc, Func<string, T> nameToEnumFunc, string schema = null)
    {
      var entry = new EnumEntry();
      entry.Type = typeof(T);
      entry.EnumName = enumName;
      entry.EnumToNameFunc = o => enumToNameFunc((T)o);
      entry.NameToEnumFunc = name => nameToEnumFunc(name);
      entry.Schema = schema;

      entry.ValueType = new KDPgValueTypeEnum(entry);

      Entries.Add(entry);
    }

    public static void RegisterNativeEnum<T>(string enumName, string schema = null) where T : struct, Enum
    {
      if (!typeof(T).IsEnum)
        throw new ArgumentException("T must be an enumerated type");

      RegisterEnum<T>(
          enumName,
          x => x.ToString(),
          Enum.Parse<T>,
          schema: schema);
    }

    public static bool HasEnumType(Type type)
    {
      // TODO: optimize
      return Entries.Exists(x => x.Type == type);
    }

    public static EnumEntry GetEnumEntryForType(Type type)
    {
      // TODO: optimize
      return Entries.Find(x => x.Type == type);
    }
  }
}