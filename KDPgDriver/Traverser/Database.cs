using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using KDPgDriver.Builders;
using KDPgDriver.Utils;

namespace KDPgDriver.Traverser
{
  internal static class Database
  {
    [SuppressMessage("ReSharper", "HeapView.ClosureAllocation")]
    [SuppressMessage("ReSharper", "HeapView.DelegateAllocation")]
    [SuppressMessage("ReSharper", "CA1822")]
    [SuppressMessage("ReSharper", "UnusedMember.Local")]
    private class Builder<T>
    {
      public void Property(Expression<Func<T, object>> prop, Func<TypedExpression, TypedExpression> processor)
      {
        PropertiesDatabase.Register(prop, processor);
      }

      public void Method<TR>(Expression<Func<T, TR>> prop,
                             Func<TypedExpression, TypedExpression> converter) =>
          MethodsDatabase.Register(prop.Body, exps => converter(exps[0]));

      public void Method<T1, TR>(Expression<Func<T, T1, TR>> prop,
                                 Func<TypedExpression, TypedExpression, TypedExpression> converter) =>
          MethodsDatabase.Register(prop.Body, exps => converter(exps[0], exps[1]));

      public void Method<T1, T2, TR>(Expression<Func<T, T1, T2, TR>> prop,
                                     Func<TypedExpression, TypedExpression, TypedExpression, TypedExpression> converter) =>
          MethodsDatabase.Register(prop.Body, exps => converter(exps[0], exps[1], exps[2]));

      public void Method<T1, T2, T3, TR>(Expression<Func<T, T1, T2, T3, TR>> prop,
                                         Func<TypedExpression, TypedExpression, TypedExpression, TypedExpression, TypedExpression> converter) =>
          MethodsDatabase.Register(prop.Body, exps => converter(exps[0], exps[1], exps[2], exps[3]));
    }

    private static void For<T>(Action<Builder<T>> builder) => builder(new Builder<T>());

    static Database()
    {
      For<DateTime>(b => {
        b.Property(x => x.Day, exp => FuncInternal.DatePart(ExtractField.Day, exp));
        b.Property(x => x.Month, exp => FuncInternal.DatePart(ExtractField.Month, exp));
        b.Property(x => x.Year, exp => FuncInternal.DatePart(ExtractField.Year, exp));
        b.Property(x => x.Hour, exp => FuncInternal.DatePart(ExtractField.Hour, exp));
        b.Property(x => x.Minute, exp => FuncInternal.DatePart(ExtractField.Minute, exp));
        b.Property(x => x.Second, exp => FuncInternal.DatePart(ExtractField.Second, exp));
        b.Property(x => x.Millisecond, exp => FuncInternal.DatePart(ExtractField.Milliseconds, exp));
        b.Property(x => x.Date, FuncInternal.Date);
      });

      For<string>(b => {
        b.Method<int, int, string>((x, start, cnt) => x.Substring(start, cnt), ExpressionBuilders.Substring);
        b.Method<string, bool>((x, str) => x.StartsWith(str), ExpressionBuilders.StartsWith);
        b.Method<string, bool>((x, str) => x.EndsWith(str), ExpressionBuilders.EndsWith);
        b.Method<string, bool>((x, str) => x.Contains(str), ExpressionBuilders.Contains);
        b.Method<string>(x => x.ToUpper(), ExpressionBuilders.ToUpper);
        b.Method<string>(x => x.ToLower(), ExpressionBuilders.ToLower);

        b.Method<string, bool>((x, str) => x.PgLike(str), ExpressionBuilders.Like);
        b.Method<string, bool>((x, str) => x.PgILike(str), ExpressionBuilders.ILike);
        b.Method<string, bool>((x, str) => x.PgRawLike(str), ExpressionBuilders.RawLike);
        b.Method<string, bool>((x, str) => x.PgRawILike(str), ExpressionBuilders.RawILike);
      });

      MethodsDatabase.Optimize();
      PropertiesDatabase.Optimize();
    }

    public static MethodsDatabase.Entry FindMethod(MethodCallExpression call)
    {
      return MethodsDatabase.EntriesMap.GetValueOrDefault(Tuple.Create(call.Method.DeclaringType, call.Method.Name));
    }

    public static PropertiesDatabase.Entry FindProperty(PropertyInfo propertyInfo)
    {
      return PropertiesDatabase.EntriesMap.GetValueOrDefault(Tuple.Create(propertyInfo.DeclaringType, propertyInfo.Name));
    }
  }
}