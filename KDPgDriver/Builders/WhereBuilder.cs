using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection.Metadata;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  public interface IWhereBuilder
  {
    RawQuery GetRawQuery();
  }

  public class WhereBuilder<TModel> : IWhereBuilder
  {
    public static WhereBuilder<TModel> Empty => new WhereBuilder<TModel>();

    private RawQuery _rawQuery = new RawQuery();

    public RawQuery GetRawQuery() => _rawQuery;

    public WhereBuilder() { }

    public WhereBuilder(RawQuery rq)
    {
      _rawQuery = rq;
    }

    public WhereBuilder<TModel> AndWith(WhereBuilder<TModel> other)
    {
      if (_rawQuery.IsEmpty)
        _rawQuery = other._rawQuery;
      else
        _rawQuery = And(this, other)._rawQuery;
      return this;
    }

    public WhereBuilder<TModel> OrWith(WhereBuilder<TModel> other)
    {
      if (_rawQuery.IsEmpty)
        _rawQuery = other._rawQuery;
      else
        _rawQuery = Or(this, other)._rawQuery;
      return this;
    }

    public static WhereBuilder<TModel> FromExpression(Expression<Func<TModel, bool>> exp)
    {
      return FromTypedExpression(NodeVisitor.VisitFuncExpression(exp));
    }

    public static WhereBuilder<TModel> Eq<T>(Expression<Func<TModel, T>> field, T value)
    {
      var column = NodeVisitor.EvaluateExpressionToColumn(field.Body);
      var pgValue = Helper.ConvertObjectToPgValue(value);

      var right = TypedExpression.FromPgValue(pgValue);

      return FromTypedExpression(ExpressionBuilders.Eq(column.TypedExpression, right));
    }

    public static WhereBuilder<TModel> In<T>(Expression<Func<TModel, T>> field, IEnumerable<T> array)
    {
      var column = NodeVisitor.EvaluateExpressionToColumn(field.Body);
      return FromTypedExpression(ExpressionBuilders.In(column.TypedExpression, array));
    }

    public static WhereBuilder<TModel> ContainsAny<T>(Expression<Func<TModel, IList<T>>> field, params T[] values)
    {
      var column = NodeVisitor.EvaluateExpressionToColumn(field.Body);
      return FromTypedExpression(ExpressionBuilders.ContainsAny(column.TypedExpression, values));
    }

    public static WhereBuilder<TModel> ContainsAny<T>(Expression<Func<TModel, IList<T>>> field, IEnumerable<T> array)
    {
      var column = NodeVisitor.EvaluateExpressionToColumn(field.Body);
      return FromTypedExpression(ExpressionBuilders.ContainsAny(column.TypedExpression, array));
    }

    public static WhereBuilder<TModel> Or(params WhereBuilder<TModel>[] statements)
    {
      var b = new WhereBuilder<TModel>();

      bool first = true;
      foreach (var statement in statements) {
        if (statement._rawQuery.IsEmpty)
          continue;

        if (!first)
          b._rawQuery.Append(" OR ");

        b._rawQuery.AppendSurround(statement._rawQuery);
        first = false;
      }

      return b;
    }

    public static WhereBuilder<TModel> And(params WhereBuilder<TModel>[] statements)
    {
      var b = new WhereBuilder<TModel>();

      bool first = true;
      foreach (var statement in statements) {
        if (statement._rawQuery.IsEmpty)
          continue;

        if (!first)
          b._rawQuery.Append(" AND ");

        b._rawQuery.AppendSurround(statement._rawQuery);
        first = false;
      }

      return b;
    }

    internal static WhereBuilder<TModel> FromTypedExpression(TypedExpression exp)
    {
      return new WhereBuilder<TModel>(exp.RawQuery);
    }
  }
}