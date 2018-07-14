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
  public class InsertQueryFluentBuilder1<TModel> : IQuery
  {
    private readonly IQueryExecutor _executor;

    private InsertQuery<TModel> _insertQuery = new InsertQuery<TModel>();

    public InsertQueryFluentBuilder1() { }

    public InsertQueryFluentBuilder1(IQueryExecutor executor)
    {
      _executor = executor;
    }

    public InsertQueryFluentBuilder1<TModel> UseField(Expression<Func<TModel, object>> field)
    {
      _insertQuery.UseField(field);
      return this;
    }

    public InsertQueryFluentBuilder1<TModel> AddObject(TModel obj)
    {
      _insertQuery.AddObject(obj);
      return this;
    }

    public InsertQueryFluentBuilder1<TModel> AddMany(IEnumerable<TModel> objs)
    {
      _insertQuery.AddMany(objs);
      return this;
    }

    // 
    public InsertQuery<TModel> GetInsertQuery()
    {
      return _insertQuery;
    }

    public async Task<InsertQueryResult> ExecuteAsync()
    {
      return await _executor.QueryAsync(GetInsertQuery());
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      return GetInsertQuery().GetRawQuery(defaultSchema);
    }
  }
}