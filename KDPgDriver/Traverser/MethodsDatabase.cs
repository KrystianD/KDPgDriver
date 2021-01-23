using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using KDPgDriver.Builders;
using KDPgDriver.Utils;

namespace KDPgDriver.Traverser
{
  internal static class MethodsDatabase
  {
    public class Entry
    {
      private readonly bool _isMethod;

      public Type Type { get; }
      public string Member { get; }
      private Func<TypedExpression[], TypedExpression> Processor { get; }

      public Entry(Type type, string member, bool isMethod, Func<TypedExpression[], TypedExpression> processor)
      {
        _isMethod = isMethod;
        Type = type;
        Member = member;
        Processor = processor;
      }

      public TypedExpression Process(MethodCallExpression call, Func<Expression, TypedExpression> expVisitor)
      {
        if (_isMethod) { // normal method
          TypedExpression callObject = expVisitor(call.Object);

          var params1 = Enumerable.Empty<TypedExpression>()
                                  .Concat(new[] { callObject })
                                  .Concat(call.Arguments.Select(expVisitor))
                                  .ToArray();

          return Processor(params1);
        }
        else { // extension
          var params1 = call.Arguments.Select(expVisitor).ToArray();

          return Processor(params1);
        }
      }
    }

    [SuppressMessage("ReSharper", "CA1822")]
    [SuppressMessage("ReSharper", "HeapView.DelegateAllocation")]
    [SuppressMessage("ReSharper", "HeapView.ClosureAllocation")]
    [SuppressMessage("ReSharper", "UnusedMember.Local")]
    private class Builder<T>
    {
      private static void Add(Expression exp, Func<TypedExpression[], TypedExpression> func)
      {
        var call = ((MethodCallExpression)exp);
        var methodName = call.Method.Name;
        _entries.Add(new Entry(typeof(T), methodName, call.Object != null, func));
      }

      public void For<TR>(Expression<Func<T, TR>> prop,
                          Func<TypedExpression, TypedExpression> converter) =>
          Add(prop.Body, exps => converter(exps[0]));

      public void For<T1, TR>(Expression<Func<T, T1, TR>> prop,
                              Func<TypedExpression, TypedExpression, TypedExpression> converter) =>
          Add(prop.Body, exps => converter(exps[0], exps[1]));

      public void For<T1, T2, TR>(Expression<Func<T, T1, T2, TR>> prop,
                                  Func<TypedExpression, TypedExpression, TypedExpression, TypedExpression> converter) =>
          Add(prop.Body, exps => converter(exps[0], exps[1], exps[2]));

      public void For<T1, T2, T3, TR>(Expression<Func<T, T1, T2, T3, TR>> prop,
                                      Func<TypedExpression, TypedExpression, TypedExpression, TypedExpression, TypedExpression> converter) =>
          Add(prop.Body, exps => converter(exps[0], exps[1], exps[2], exps[3]));
    }

    private static Builder<T> BuildFor<T>() => new Builder<T>();

    private static readonly List<Entry> _entries = new List<Entry>();
    public static readonly Dictionary<Tuple<Type, string>, Entry> EntriesMap;

    static MethodsDatabase()
    {
      BuildFor<string>().For<int, int, string>((x, a, b) => x.Substring(a, b), ExpressionBuilders.Substring);
      BuildFor<string>().For<string, bool>((x, a) => x.StartsWith(a), ExpressionBuilders.StartsWith);
      BuildFor<string>().For<string, bool>((x, a) => x.EndsWith(a), ExpressionBuilders.EndsWith);
      BuildFor<string>().For<string, bool>((x, a) => x.Contains(a), ExpressionBuilders.Contains);
      BuildFor<string>().For<string>((x) => x.ToUpper(), ExpressionBuilders.ToUpper);
      BuildFor<string>().For<string>((x) => x.ToLower(), ExpressionBuilders.ToLower);

      BuildFor<string>().For<string, bool>((x, a) => x.PgLike(a), ExpressionBuilders.Like);
      BuildFor<string>().For<string, bool>((x, a) => x.PgILike(a), ExpressionBuilders.ILike);
      BuildFor<string>().For<string, bool>((x, a) => x.PgRawLike(a), ExpressionBuilders.RawLike);
      BuildFor<string>().For<string, bool>((x, a) => x.PgRawILike(a), ExpressionBuilders.RawILike);

      EntriesMap = _entries.ToDictionary(x => Tuple.Create(x.Type, x.Member));
    }
  }
}