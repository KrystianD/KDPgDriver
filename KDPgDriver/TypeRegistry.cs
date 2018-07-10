using System;
using System.Collections.Generic;
using Npgsql;
using Npgsql.NameTranslation;

namespace KDPgDriver
{
  public static class TypeRegistry
  {
    public class EnumEntry
    {
      public Type type;
      public string enumName;
      public Func<object, string> enumToNameFunc;
      public Func<string, object> nameToEnumFunc;
    }

    private static List<EnumEntry> entries = new List<EnumEntry>();

    public static void RegisterEnum<T>(string enumName, Func<T, string> enumToNameFunc, Func<string, T> nameToEnumFunc)
    {
      var entry = new EnumEntry();
      entry.type = typeof(T);
      entry.enumName = enumName;
      entry.enumToNameFunc = o => enumToNameFunc((T) o);
      entry.nameToEnumFunc = name => nameToEnumFunc(name);

      entries.Add(entry);
    }

    public static bool HasEnumType(Type type)
    {
      // TODO: optimize
      return entries.Exists(x => x.type == type);
    }

    public static EnumEntry GetEnumEntryForType(Type type)
    {
      // TODO: optimize
      return entries.Find(x => x.type == type);
    }
  }
}