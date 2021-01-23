using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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

    public static void Register(Expression exp, Func<TypedExpression[], TypedExpression> func)
    {
      var call = ((MethodCallExpression)exp);
      var methodName = call.Method.Name;
      _entries.Add(new Entry(call.Method.DeclaringType, methodName, call.Object != null, func));
    }

    private static readonly List<Entry> _entries = new List<Entry>();
    public static Dictionary<Tuple<Type, string>, Entry> EntriesMap;

    public static void Optimize()
    {
      EntriesMap = _entries.ToDictionary(x => Tuple.Create(x.Type, x.Member));
    }
  }
}