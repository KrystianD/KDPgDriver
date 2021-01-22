using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.Serialization;
using JetBrains.Annotations;
using KDLib;
using KDPgDriver.Builders;
using KDPgDriver.Utils;

// ReSharper disable UnusedMember.Global

namespace KDPgDriver
{
  [SuppressMessage("ReSharper", "StringLiteralTypo")]
  public enum ExtractField
  {
    [EnumMember(Value = "century")]
    Century,

    [EnumMember(Value = "day")]
    Day,

    [EnumMember(Value = "decade")]
    Decade,

    [EnumMember(Value = "dow")]
    Dow,

    [EnumMember(Value = "doy")]
    Doy,

    [EnumMember(Value = "epoch")]
    Epoch,

    [EnumMember(Value = "hour")]
    Hour,

    [EnumMember(Value = "isodow")]
    IsoDayOfWeek,

    [EnumMember(Value = "isoyear")]
    IsoYear,

    [EnumMember(Value = "microseconds")]
    Microseconds,

    [EnumMember(Value = "millennium")]
    Millennium,

    [EnumMember(Value = "milliseconds")]
    Milliseconds,

    [EnumMember(Value = "minute")]
    Minute,

    [EnumMember(Value = "month")]
    Month,

    [EnumMember(Value = "quarter")]
    Quarter,

    [EnumMember(Value = "second")]
    Second,

    [EnumMember(Value = "timezone")]
    Timezone,

    [EnumMember(Value = "timezone_hour")]
    TimezoneHour,

    [EnumMember(Value = "timezone_minute")]
    TimezoneMinute,

    [EnumMember(Value = "week")]
    Week,

    [EnumMember(Value = "year")]
    Year,
  }

  internal static class FuncInternal
  {
    public static TypedExpression MD5(TypedExpression query)
    {
      var rq = RawQuery.Create("MD5(").Append(query.RawQuery).Append(")");
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new TypedExpression(rq, KDPgValueTypeInstances.String);
    }

    public static TypedExpression Count(TypedExpression query)
    {
      var rq = RawQuery.Create("COUNT(").Append(query.RawQuery).Append(")");
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new TypedExpression(rq, KDPgValueTypeInstances.Integer64);
    }

    public static TypedExpression Now()
    {
      var rq = RawQuery.Create("NOW()");
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new TypedExpression(rq, KDPgValueTypeInstances.Time);
    }

    public static TypedExpression Coalesce(TypedExpression value1,
                                           TypedExpression value2 = null,
                                           TypedExpression value3 = null,
                                           TypedExpression value4 = null,
                                           TypedExpression value5 = null)
    {
      var rq = RawQuery.Create("COALESCE(");
      rq.Append(value1.RawQuery);

      if (value2 != null) {
        rq.Append(", ");
        rq.Append(value2.RawQuery);
      }

      if (value3 != null) {
        rq.Append(", ");
        rq.Append(value3.RawQuery);
      }

      if (value4 != null) {
        rq.Append(", ");
        rq.Append(value4.RawQuery);
      }

      if (value5 != null) {
        rq.Append(", ");
        rq.Append(value5.RawQuery);
      }

      rq.Append(")");

      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new TypedExpression(rq, value1.Type);
    }

    public static TypedExpression GetVariableInt(string name)
    {
      return ExpressionBuilders.GetConfigInt(name);
    }

    public static TypedExpression GetVariableText(string name)
    {
      return ExpressionBuilders.GetConfigText(name);
    }

    public static TypedExpression Date(TypedExpression query)
    {
      var rq = RawQuery.Create("DATE(").Append(query.RawQuery).Append(")");
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new TypedExpression(rq, KDPgValueTypeInstances.Date);
    }

    public static TypedExpression Extract(ExtractField field, TypedExpression source)
    {
      var rq = RawQuery.Create("EXTRACT(").AppendStringValue(field.GetMemberValue()).Append(" FROM ").Append(source.RawQuery).Append(")");
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new TypedExpression(rq, KDPgValueTypeInstances.Decimal);
    }

    public static TypedExpression DatePart(ExtractField field, TypedExpression source)
    {
      var rq = RawQuery.Create("date_part(").AppendStringValue(field.GetMemberValue()).Append(",").Append(source.RawQuery).Append(")");
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new TypedExpression(rq, KDPgValueTypeInstances.Decimal);
    }

    public static TypedExpression Timezone(TypedExpression zone, TypedExpression timestamp)
    {
      var rq = RawQuery.Create("timezone(").Append(zone.RawQuery).Append(",").Append(timestamp.RawQuery).Append(")");
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new TypedExpression(rq, KDPgValueTypeInstances.DateTime);
    }
  }

  public static class Func
  {
    public static string MD5([UsedImplicitly] string value)
    {
      throw new Exception("do not use directly");
    }

    public static long Count([UsedImplicitly] object value)
    {
      throw new Exception("do not use directly");
    }

    public static DateTime Now()
    {
      throw new Exception("do not use directly");
    }

    public static T Raw<T>([UsedImplicitly] string text)
    {
      throw new Exception("do not use directly");
    }

    public static T Coalesce<T>([UsedImplicitly] T value1)
    {
      throw new Exception("do not use directly");
    }

    public static T Coalesce<T>([UsedImplicitly] T value1, [UsedImplicitly] T value2)
    {
      throw new Exception("do not use directly");
    }

    public static T Coalesce<T>([UsedImplicitly] T value1, [UsedImplicitly] T value2, [UsedImplicitly] T value3)
    {
      throw new Exception("do not use directly");
    }

    public static T Coalesce<T>([UsedImplicitly] T value1, [UsedImplicitly] T value2, [UsedImplicitly] T value3, [UsedImplicitly] T value4)
    {
      throw new Exception("do not use directly");
    }

    public static T Coalesce<T>([UsedImplicitly] T value1, [UsedImplicitly] T value2, [UsedImplicitly] T value3, [UsedImplicitly] T value4, [UsedImplicitly] T value5)
    {
      throw new Exception("do not use directly");
    }

    public static int GetVariableInt([UsedImplicitly] string name)
    {
      throw new Exception("do not use directly");
    }

    public static string GetVariableText([UsedImplicitly] string name)
    {
      throw new Exception("do not use directly");
    }

    public static DateTime Date([UsedImplicitly] DateTime date)
    {
      throw new Exception("do not use directly");
    }

    public static double Extract([UsedImplicitly] ExtractField field, [UsedImplicitly] DateTime source)
    {
      throw new Exception("do not use directly");
    }

    public static double Extract([UsedImplicitly] ExtractField field, [UsedImplicitly] TimeSpan source)
    {
      throw new Exception("do not use directly");
    }

    public static double DatePart([UsedImplicitly] ExtractField field, [UsedImplicitly] DateTime source)
    {
      throw new Exception("do not use directly");
    }

    public static double DatePart([UsedImplicitly] ExtractField field, [UsedImplicitly] TimeSpan source)
    {
      throw new Exception("do not use directly");
    }

    public static DateTime Timezone([UsedImplicitly] string zone, [UsedImplicitly] DateTime timestamp)
    {
      throw new Exception("do not use directly");
    }
  }
}