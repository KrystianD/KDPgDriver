using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using KDPgDriver.Builders;
using KDPgDriver.Utils;

namespace KDPgDriver.Traverser
{
  internal static class PropertiesDatabase
  {
    public class Entry
    {
      public Type Type { get; }
      public string Member { get; }
      public Func<TypedExpression, TypedExpression> Processor { get; }

      public Entry(Type type, string member, Func<TypedExpression, TypedExpression> processor)
      {
        Type = type;
        Member = member;
        Processor = processor;
      }
    }

    public static void Register<T>(Expression<Func<T, object>> prop, Func<TypedExpression, TypedExpression> processor)
    {
      var memberExpression = ((UnaryExpression)prop.Body).Operand;
      var memberInfo = ((MemberExpression)memberExpression).Member;

      switch (memberInfo) {
        case PropertyInfo propertyInfo:
          _entries.Add(new Entry(typeof(T), propertyInfo.Name, exp => ExpressionBuilders.Cast(processor(exp), propertyInfo.PropertyType)));
          return;
        default:
          throw new Exception();
      }
    }

    private static readonly List<Entry> _entries = new List<Entry>();
    public static Dictionary<Tuple<Type, string>, Entry> EntriesMap;

    public static void Optimize()
    {
      EntriesMap = _entries.ToDictionary(x => Tuple.Create(x.Type, x.Member));
    }
  }
}