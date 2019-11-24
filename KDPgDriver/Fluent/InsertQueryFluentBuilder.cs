using System;
using System.Collections.Generic;
using System.Diagnostics;
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

    public InsertQueryFluentBuilder1()
    {
    }

    public InsertQueryFluentBuilder1(IQueryExecutor executor)
    {
      _executor = executor;
    }

    public InsertQueryFluentBuilder1<TModel> UseField(Expression<Func<TModel, object>> field)
    {
      _insertQuery.UseField(field);
      return this;
    }

    public InsertQueryFluentBuilder1<TModel> UsePreviousInsertId<TRefModel>(Expression<Func<TModel, object>> field, Expression<Func<TRefModel, int>> idField)
    {
      _insertQuery.UsePreviousInsertId<TRefModel>(field, idField);
      return this;
    }

    public InsertQueryFluentBuilder1<TModel> IntoVariable(string name)
    {
      _insertQuery.IntoVariable(name);
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

    public InsertQueryFluentBuilder1<TModel> OnConflictDoNothing()
    {
      _insertQuery.OnConflictDoNothing();
      return this;
    }

    public InsertQueryFluentBuilder1<TModel> OnConflictDoUpdate(Action<FieldListBuilder<TModel>> fields, Action<UpdateStatementsBuilder<TModel>> builder)
    {
      _insertQuery.OnConflictDoUpdate(fields, builder);
      return this;
    }

    // 
    public InsertQuery<TModel> GetInsertQuery()
    {
      return _insertQuery;
    }

    public async Task<InsertQueryResult> ExecuteAsync()
    {
      if (_insertQuery.IsEmpty)
        return new InsertQueryResult(null);

      return await _executor.QueryAsync(GetInsertQuery());
    }

    public async Task<int> ExecuteForIdAsync()
    {
      if (_insertQuery.IsEmpty)
        throw new Exception("Cannot insert empty list for id");

      var res = await _executor.QueryAsync(GetInsertQuery());
      Debug.Assert(res.LastInsertId != null, "res.LastInsertId != null");
      return res.LastInsertId.Value;
    }

    public void Schedule()
    {
      if (_insertQuery.IsEmpty)
        return;

      _executor.ScheduleQuery(GetInsertQuery());
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      return GetInsertQuery().GetRawQuery(defaultSchema);
    }
  }
}