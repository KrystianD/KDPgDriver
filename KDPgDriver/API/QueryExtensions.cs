using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using KDPgDriver.Queries;

namespace KDPgDriver
{
  public static class QueryExtensions
  {
    public static bool PgIn<T>(this T source, [ItemCanBeNull] params T[] values)
    {
      throw new Exception("do not use directly");
    }

    public static bool PgIn<T>(this T source, [ItemCanBeNull] IEnumerable<T> values)
    {
      throw new Exception("do not use directly");
    }

    public static bool PgIn<T>(this T source, SelectSubquery<T?> subquery) where T : struct
    {
      throw new Exception("do not use directly");
    }

    public static bool PgIn<T>(this T source, SelectSubquery<T> subquery)
    {
      throw new Exception("do not use directly");
    }

    public static bool PgNotIn<T>(this T source, [ItemCanBeNull] params T[] values)
    {
      throw new Exception("do not use directly");
    }

    public static bool PgNotIn<T>(this T source, [ItemCanBeNull] IEnumerable<T> values)
    {
      throw new Exception("do not use directly");
    }

    public static bool PgNotIn<T>(this T source, SelectSubquery<T?> subquery) where T : struct
    {
      throw new Exception("do not use directly");
    }

    public static bool PgNotIn<T>(this T source, SelectSubquery<T> subquery)
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

    public static bool PgLike(this string source, string value)
    {
      throw new Exception("do not use directly");
    }

    public static bool PgILike(this string source, string value)
    {
      throw new Exception("do not use directly");
    }

    public static bool PgRawLike(this string source, string value)
    {
      throw new Exception("do not use directly");
    }

    public static bool PgRawILike(this string source, string value)
    {
      throw new Exception("do not use directly");
    }
  }
}