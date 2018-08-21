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
  public class UpdateQueryFluentBuilder1<TModel> : IQuery
  {
    private readonly IQueryExecutor _executor;

    private readonly WhereBuilder<TModel> _whereBuilder = WhereBuilder<TModel>.Empty;
    private readonly UpdateStatementsBuilder<TModel> _updateStatementsBuilder = new UpdateStatementsBuilder<TModel>();

    public UpdateQueryFluentBuilder1() { }

    public UpdateQueryFluentBuilder1(IQueryExecutor executor)
    {
      _executor = executor;
    }

    public bool IsEmpty => _updateStatementsBuilder.IsEmpty;

    // Where
    public UpdateQueryFluentBuilder1<TModel> Where(Expression<Func<TModel, bool>> exp)
    {
      _whereBuilder.AndWith(WhereBuilder<TModel>.FromExpression(exp));
      return this;
    }

    public UpdateQueryFluentBuilder1<TModel> Where(WhereBuilder<TModel> builder)
    {
      _whereBuilder.AndWith(builder);
      return this;
    }

    // Update
    public UpdateQueryFluentBuilder1<TModel> SetField<TValue>(Expression<Func<TModel, TValue>> field, TValue value)
    {
      _updateStatementsBuilder.SetField(field, value);
      return this;
    }
    
    public UpdateQueryFluentBuilder1<TModel> SetField<TValue>(Expression<Func<TModel, TValue>> field, Expression<Func<TModel, TValue>> valueExpression)
    {
      _updateStatementsBuilder.SetField(field, valueExpression);
      return this;
    }

    public UpdateQueryFluentBuilder1<TModel> AddToList<TValue>(Expression<Func<TModel, IList<TValue>>> field, TValue value)
    {
      _updateStatementsBuilder.AddToList(field, value);
      return this;
    }

    public UpdateQueryFluentBuilder1<TModel> RemoveFromList<TValue>(Expression<Func<TModel, IList<TValue>>> field, TValue value)
    {
      _updateStatementsBuilder.RemoveFromList(field, value);
      return this;
    }

    // 
    public UpdateQuery<TModel> GetUpdateQuery()
    {
      return new UpdateQuery<TModel>(_whereBuilder, _updateStatementsBuilder);
    }

    public async Task ExecuteAsync()
    {
      await _executor.QueryAsync(GetUpdateQuery());
    }

    public void Schedule()
    {
      _executor.ScheduleQuery(GetUpdateQuery());
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      return GetUpdateQuery().GetRawQuery(defaultSchema);
    }
  }
}