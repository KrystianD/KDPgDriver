using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Linq.Expressions;
using System.Reflection.Metadata;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  public class WhereBuilder<TModel>
  {
    public RawQuery RawQuery { get; private set; } = new RawQuery();

    public static WhereBuilder<TModel> Empty => new WhereBuilder<TModel>();

    public WhereBuilder<TModel> AndWith(WhereBuilder<TModel> other)
    {
      RawQuery = And(this, other).RawQuery;
      return this;
    }

    public WhereBuilder<TModel> OrWith(WhereBuilder<TModel> other)
    {
      RawQuery = Or(this, other).RawQuery;
      return this;
    }

    public static WhereBuilder<TModel> Eq<T>(Expression<Func<TModel, T>> field, T value)
    {
      var name = NodeVisitor.VisitProperty(field.Body);
      var pgValue = Helper.ConvertObjectToPgValue(value);

      var b = new WhereBuilder<TModel>();
      b.RawQuery.AppendSurround(Helper.QuoteObjectName(name)).Append(" = ").AppendSurround(pgValue);

      return b;
    }

    public static WhereBuilder<TModel> In<T>(Expression<Func<TModel, T>> field, IEnumerable<T> array)
    {
      var name = NodeVisitor.VisitProperty(field.Body);

      var b = new WhereBuilder<TModel>();
      b.RawQuery.AppendSurround(Helper.QuoteObjectName(name)).Append(" = ANY(");
      b.RawQuery.Append(Helper.ConvertObjectToPgValue(array));
      b.RawQuery.Append(")");

      return b;
    }

    public static WhereBuilder<TModel> ContainsAny<T>(Expression<Func<TModel, IList<T>>> field, params T[] values)
      => ContainsAny(field, (IEnumerable<T>) values);

    public static WhereBuilder<TModel> ContainsAny<T>(Expression<Func<TModel, IList<T>>> field, IEnumerable<T> array)
    {
      var name = NodeVisitor.VisitProperty(field.Body);

      var b = new WhereBuilder<TModel>();
      b.RawQuery.AppendSurround(Helper.ConvertObjectToPgValue(array)).Append(" && ").AppendSurround(Helper.QuoteObjectName(name));

      return b;
    }

    public static WhereBuilder<TModel> Or(params WhereBuilder<TModel>[] statements)
    {
      var b = new WhereBuilder<TModel>();

      bool first = true;
      foreach (var statement in statements) {
        if (statement.RawQuery.IsEmpty)
          continue;

        if (!first)
          b.RawQuery.Append(" OR ");

        b.RawQuery.AppendSurround(statement.RawQuery);
        first = false;
      }

      return b;
    }

    public static WhereBuilder<TModel> And(params WhereBuilder<TModel>[] statements)
    {
      var b = new WhereBuilder<TModel>();

      bool first = true;
      foreach (var statement in statements) {
        if (statement.RawQuery.IsEmpty)
          continue;

        if (!first)
          b.RawQuery.Append(" AND ");

        b.RawQuery.AppendSurround(statement.RawQuery);
        first = false;
      }

      return b;
    }
  }
}