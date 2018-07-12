using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Cryptography;
using System.Text;
using KDPgDriver.Queries;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders
{
  public class QueryBuilder<TModel> : IQueryBuilder
  {
    public string TableName { get; }
    public string SchemaName { get; }

    private readonly WhereBuilder<TModel> _wherePart = WhereBuilder<TModel>.Empty;

    public IWhereBuilder GetWhereBuilder() => _wherePart;

    public QueryBuilder()
    {
      TableName = Helper.GetTableName(typeof(TModel));
      SchemaName = Helper.GetTableSchema(typeof(TModel));
    }

    public QueryBuilder<TModel> Where(Expression<Func<TModel, bool>> exp)
    {
      _wherePart.AndWith(WhereBuilder<TModel>.FromExpression(exp));
      return this;
    }

    public QueryBuilder<TModel> Where(WhereBuilder<TModel> builder)
    {
      _wherePart.AndWith(builder);
      return this;
    }

    public SelectQuery<TModel> Select()
    {
      return new SelectQuery<TModel>(this, SelectFromBuilder.AllColumns<TModel>(), null, null);
    }

    public SelectQuery<TNewModel> Select<TNewModel>(Expression<Func<TModel, TNewModel>> pr)
    {
      return new SelectQuery<TNewModel>(this, SelectFromBuilder.FromExpression(pr), null, null);
    }

    public SelectQuery<TModel> SelectOnly(FieldListBuilder<TModel> builder)
    {
      return new SelectQuery<TModel>(this, SelectFromBuilder.FromFieldListBuilder(builder), null, null);
    }

    public SelectQuery<TModel> SelectOnly(params Expression<Func<TModel, object>>[] fieldsList)
    {
      var builder = new FieldListBuilder<TModel>();
      foreach (var expression in fieldsList)
        builder.AddField(expression);
      return SelectOnly(builder);
    }

    public UpdateQuery<TModel> Update(UpdateStatementsBuilder<TModel> builder)
    {
      if (builder.IsEmpty)
        throw new Exception("Empty update statement builder");
      var uq = new UpdateQuery<TModel>(this, builder);
      return uq;
    }

    public DeleteQuery Delete()
    {
      return new DeleteQuery(this);
    }
  }
}