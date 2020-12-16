using System;
using System.Collections.Generic;
using KDPgDriver.Queries;

namespace KDPgDriver.Utils
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

    public static bool PgIn<T>(this T source, SelectSubquery<T> subquery)
    {
      throw new Exception("do not use directly");
    }

    public static bool PgNotIn<T>(this T source, params T[] values)
    {
      throw new Exception("do not use directly");
    }

    public static bool PgNotIn<T>(this T source, IEnumerable<T> values)
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