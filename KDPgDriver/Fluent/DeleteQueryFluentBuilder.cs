using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using KDPgDriver.Builders;
using KDPgDriver.Queries;
using KDPgDriver.Results;
using KDPgDriver.Utils;

namespace KDPgDriver.Fluent
{
  public class DeleteQueryFluentBuilder1<TModel> : IQuery
  {
    private readonly IQueryExecutor _executor;

    private readonly WhereBuilder<TModel> _whereBuilder = WhereBuilder<TModel>.Empty;

    public DeleteQueryFluentBuilder1() { }

    public DeleteQueryFluentBuilder1(IQueryExecutor executor)
    {
      _executor = executor;
    }

    // Where
    public DeleteQueryFluentBuilder1<TModel> Where(Expression<Func<TModel, bool>> exp)
    {
      _whereBuilder.AndWith(WhereBuilder<TModel>.FromExpression(exp));
      return this;
    }

    public DeleteQueryFluentBuilder1<TModel> Where(WhereBuilder<TModel> builder)
    {
      _whereBuilder.AndWith(builder);
      return this;
    }

    // 
    public DeleteQuery<TModel> GetDeleteQuery()
    {
      return new DeleteQuery<TModel>(_whereBuilder);
    }

    public async Task ExecuteAsync()
    {
      await _executor.QueryAsync(GetDeleteQuery());
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      return GetDeleteQuery().GetRawQuery(defaultSchema);
    }
  }
}