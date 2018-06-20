using System;
using System.Collections.Generic;

namespace KDPgDriver
{
  public static class QueryExtensions
  {
    public static bool PgIn<T>(this T source, params T[] values)
    {
      throw new Exception("do not use directly");
    }

    public static bool PgIn<T>(this T source, IEnumerable<T> values)
    {
      throw new Exception("do not use directly");
    }

    public static bool PgContainsAny<T>(this IEnumerable<T> source, IEnumerable<T> values)
    {
      throw new Exception("do not use directly");
    }

    public static bool PgContainsAny<T>(this IEnumerable<T> source, params T[] values)
    {
      throw new Exception("do not use directly");
    }
  }
}