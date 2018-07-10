using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  public class QueryBuilder<TModel> : IQueryBuilder
  {
    // public Driver Driver { get; }
    public string TableName { get; }
    public string SchemaName { get; }

    private readonly RawQuery _wherePart = new RawQuery();

    public RawQuery GetWherePart() => _wherePart;

    public QueryBuilder()
    {
      // Driver = driver;
      TableName = Helper.GetTableName(typeof(TModel));
      SchemaName = Helper.GetTableSchema(typeof(TModel));
    }

    public QueryBuilder<TModel> Where(Expression<Func<TModel, bool>> exp)
    {
      var whereSql = NodeVisitor.Visit(exp.Body, exp.Parameters.First().Name).RawQuery;

      if (!_wherePart.IsEmpty)
        _wherePart.Append(" AND ");
      _wherePart.Append("(");
      _wherePart.Append(whereSql);
      _wherePart.Append(")");

      return this;
    }

    public QueryBuilder<TModel> Where(WhereBuilder<TModel> builder)
    {
      if (!_wherePart.IsEmpty)
        _wherePart.Append(" AND ");
      _wherePart.Append("(");
      _wherePart.Append(builder.RawQuery);
      _wherePart.Append(")");

      return this;
    }

    public SelectQuery<TModel> Select()
    {
      return new SelectQuery<TModel>(this);
    }

    public SelectQuery<TNewModel> Select<TNewModel>(Expression<Func<TModel, TNewModel>> pr)
    {
      var us = new SelectQuery<TNewModel>(this);
      us.ProcessSingleField(pr);
      return us;
    }

    public SelectQuery<TModel> Select(FieldListBuilder<TModel> builder)
    {
      var us = new SelectQuery<TModel>(this);
      us.ProcessListOfFields(builder);
      return us;
    }

    public SelectQuery<TModel> SelectFields(params Expression<Func<TModel, object>>[] fieldsList)
    {
      var builder = new FieldListBuilder<TModel>();
      foreach (var expression in fieldsList)
        builder.AddField(expression);
      return Select(builder);
    }

    public UpdateQuery<TModel> Update(UpdateStatementsBuilder<TModel> builder)
    {
      if (builder.IsEmpty)
        throw new Exception("Empty update statement builder");
      var uq = new UpdateQuery<TModel>(this, builder);
      return uq;
    }

    public DeleteQuery<TModel> Delete()
    {
      return new DeleteQuery<TModel>(this);
    }
  }
}