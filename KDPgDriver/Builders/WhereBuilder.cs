using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders
{
  public interface IWhereBuilder
  {
    RawQuery GetRawQuery();
  }

  public class WhereBuilder<TModel> : IWhereBuilder
  {
    public static WhereBuilder<TModel> Empty => new WhereBuilder<TModel>();

    private TypedExpression _typedExpression = TypedExpression.Empty;

    public RawQuery GetRawQuery() => _typedExpression.RawQuery;

    public WhereBuilder() { }

    public WhereBuilder(TypedExpression rq)
    {
      _typedExpression = rq;
    }

    public WhereBuilder<TModel> AndWith(WhereBuilder<TModel> other)
    {
      if (_typedExpression.IsEmpty)
        _typedExpression = other._typedExpression;
      else
        _typedExpression = And(this, other)._typedExpression;
      return this;
    }

    public WhereBuilder<TModel> OrWith(WhereBuilder<TModel> other)
    {
      if (_typedExpression.IsEmpty)
        _typedExpression = other._typedExpression;
      else
        _typedExpression = Or(this, other)._typedExpression;
      return this;
    }

    public static WhereBuilder<TModel> FromExpression(Expression<Func<TModel, bool>> exp)
    {
      return new WhereBuilder<TModel>(NodeVisitor.VisitFuncExpression(exp));
    }

    public static WhereBuilder<TModel> Eq<T>(Expression<Func<TModel, T>> field, T value)
    {
      var column = NodeVisitor.EvaluateExpressionToColumn(field.Body);
      var pgValue = PgTypesConverter.ConvertObjectToPgValue(value);

      var right = TypedExpression.FromPgValue(pgValue);

      return new WhereBuilder<TModel>(ExpressionBuilders.Eq(column.TypedExpression, right));
    }

    public static WhereBuilder<TModel> In<T>(Expression<Func<TModel, T>> field, IEnumerable<T> array)
    {
      var column = NodeVisitor.EvaluateExpressionToColumn(field.Body);
      return new WhereBuilder<TModel>(ExpressionBuilders.In(column.TypedExpression, array));
    }

    public static WhereBuilder<TModel> ContainsAny<T>(Expression<Func<TModel, IList<T>>> field, params T[] values)
    {
      var column = NodeVisitor.EvaluateExpressionToColumn(field.Body);
      return new WhereBuilder<TModel>(ExpressionBuilders.ContainsAny(column.TypedExpression, values));
    }

    public static WhereBuilder<TModel> ContainsAny<T>(Expression<Func<TModel, IList<T>>> field, IEnumerable<T> array)
    {
      var column = NodeVisitor.EvaluateExpressionToColumn(field.Body);
      return new WhereBuilder<TModel>(ExpressionBuilders.ContainsAny(column.TypedExpression, array));
    }

    public static WhereBuilder<TModel> Or(params WhereBuilder<TModel>[] statements)
    {
      return new WhereBuilder<TModel>(ExpressionBuilders.Or(statements.Select(x => x._typedExpression)));
    }

    public static WhereBuilder<TModel> And(params WhereBuilder<TModel>[] statements)
    {
      return new WhereBuilder<TModel>(ExpressionBuilders.And(statements.Select(x => x._typedExpression)));
    }

    public static WhereBuilder<TModel> Not(WhereBuilder<TModel> statement)
    {
      return new WhereBuilder<TModel>(ExpressionBuilders.Not(statement._typedExpression));
    }
  }
}