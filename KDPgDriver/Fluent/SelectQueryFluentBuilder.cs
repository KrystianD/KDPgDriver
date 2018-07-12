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
  public class SelectQueryFluentBuilder1<TModel>
  {
    private readonly IQueryExecutor _executor;

    public SelectQueryFluentBuilder1() { }

    public SelectQueryFluentBuilder1(IQueryExecutor executor)
    {
      _executor = executor;
    }

    public SelectQueryFluentBuilder2<TModel, TModel> Select()
    {
      return new SelectQueryFluentBuilder2<TModel, TModel>(SelectFromBuilder<TModel>.AllColumns(), _executor);
    }

    public SelectQueryFluentBuilder2<TModel, TNewModel> Select<TNewModel>(Expression<Func<TModel, TNewModel>> pr)
    {
      return new SelectQueryFluentBuilder2<TModel, TNewModel>(SelectFromBuilder<TNewModel>.FromExpression(pr), _executor);
    }
  }

  public class SelectQueryFluentBuilder2<TModel, TNewModel> : IQuery
  {
    private readonly IQueryExecutor _executor;
    private readonly SelectFromBuilder<TNewModel> _selectFromBuilder;
    private readonly OrderBuilder<TModel> _orderBuilder = new OrderBuilder<TModel>();
    private readonly QueryBuilder<TModel> _queryBuilder = Builders<TModel>.Query;
    private readonly LimitBuilder _limitBuilder = new LimitBuilder();

    public SelectQueryFluentBuilder2(SelectFromBuilder<TNewModel> selectFromBuilder, IQueryExecutor executor = null)
    {
      _executor = executor;
      _selectFromBuilder = selectFromBuilder;
    }

    public SelectQueryFluentBuilder2<TModel, TNewModel> Where(Expression<Func<TModel, bool>> exp)
    {
      _queryBuilder.Where(exp);
      return this;
    }

    public SelectQueryFluentBuilder2<TModel, TNewModel> Where(WhereBuilder<TModel> builder)
    {
      _queryBuilder.Where(builder);
      return this;
    }

    public SelectQueryFluentBuilder2<TModel, TNewModel> OrderBy(Expression<Func<TModel, object>> exp)
    {
      _orderBuilder.OrderBy(exp);
      return this;
    }

    public SelectQueryFluentBuilder2<TModel, TNewModel> OrderByDescending(Expression<Func<TModel, object>> exp)
    {
      _orderBuilder.OrderByDescending(exp);
      return this;
    }

    public SelectQueryFluentBuilder2<TModel, TNewModel> Limit(int limit)
    {
      _limitBuilder.Limit(limit);
      return this;
    }

    public SelectQueryFluentBuilder2<TModel, TNewModel> Offset(int offset)
    {
      _limitBuilder.Offset(offset);
      return this;
    }

    public SelectQuery<TNewModel> GetSelectQuery()
    {
      return new SelectQuery<TNewModel>(_queryBuilder, _selectFromBuilder, _orderBuilder, _limitBuilder);
    }

    public async Task<TNewModel> ToSingleAsync()
    {
      var res = await _executor.QueryAsync(GetSelectQuery());
      return res.GetSingle();
    }

    public async Task<TNewModel> ToSingleOrDefaultAsync()
    {
      var res = await _executor.QueryAsync(GetSelectQuery());
      return res.GetSingleOrDefault();
    }

    public async Task<List<TNewModel>> ToListAsync()
    {
      var res = await _executor.QueryAsync(GetSelectQuery());
      return res.GetAll();
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      return GetSelectQuery().GetRawQuery(defaultSchema);
    }
  }
}