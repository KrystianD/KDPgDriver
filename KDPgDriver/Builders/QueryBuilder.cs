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
    private readonly WhereBuilder<TModel> _whereBuilder = WhereBuilder<TModel>.Empty;

    public IWhereBuilder GetWhereBuilder() => _whereBuilder;

    public QueryBuilder<TModel> Where(Expression<Func<TModel, bool>> exp)
    {
      _whereBuilder.AndWith(WhereBuilder<TModel>.FromExpression(exp));
      return this;
    }

    public QueryBuilder<TModel> Where(WhereBuilder<TModel> builder)
    {
      _whereBuilder.AndWith(builder);
      return this;
    }

    public SelectQuery<TModel, TModel> Select()
    {
      return new SelectQuery<TModel, TModel>(this, SelectFromBuilder.AllColumns<TModel>(), null, null);
    }

    public SelectQuery<TModel, TNewModel> Select<TNewModel>(Expression<Func<TModel, TNewModel>> pr)
    {
      return new SelectQuery<TModel, TNewModel>(this, SelectFromBuilder.FromExpression(pr), null, null);
    }

    public SelectQuery<TModel, TModel> SelectOnly(FieldListBuilder<TModel> builder)
    {
      return new SelectQuery<TModel, TModel>(this, SelectFromBuilder.FromFieldListBuilder(builder), null, null);
    }

    public SelectQuery<TModel, TModel> SelectOnly(params Expression<Func<TModel, object>>[] fieldsList)
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
      var uq = new UpdateQuery<TModel>(_whereBuilder, builder);
      return uq;
    }

    public DeleteQuery<TModel> Delete()
    {
      return new DeleteQuery<TModel>(_whereBuilder);
    }
  }
}