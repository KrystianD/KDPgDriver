using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using KDPgDriver.Builders;

namespace KDPgDriver
{
  public class SelectQueryFluentBuilder1<TModel>
  {
    private readonly Driver _driver;

    public SelectQueryFluentBuilder1(Driver driver)
    {
      _driver = driver;
    }

    public SelectQueryFluentBuilder2<TModel, TModel> Select()
    {
      return new SelectQueryFluentBuilder2<TModel, TModel>(_driver, SelectFromBuilder<TModel>.AllColumns());
    }

    public SelectQueryFluentBuilder2<TModel, TNewModel> Select<TNewModel>(Expression<Func<TModel, TNewModel>> pr)
    {
      return new SelectQueryFluentBuilder2<TModel, TNewModel>(_driver, SelectFromBuilder<TNewModel>.FromExpression(pr));
    }
  }

  public class SelectQueryFluentBuilder2<TModel, TNewModel> : IQuery
  {
    private readonly Driver _driver;
    private readonly SelectFromBuilder<TNewModel> _selectFromBuilder;
    private readonly OrderBuilder<TModel> _orderBuilder = new OrderBuilder<TModel>();
    private readonly QueryBuilder<TModel> _queryBuilder = Builders<TModel>.Query;
    private readonly LimitBuilder _limitBuilder = new LimitBuilder();

    public SelectQueryFluentBuilder2(Driver driver, SelectFromBuilder<TNewModel> selectFromBuilder)
    {
      _driver = driver;
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
      var res = await _driver.QueryAsync(GetSelectQuery());
      return await res.GetSingle();
    }

    public async Task<TNewModel> ToSingleOrDefaultAsync()
    {
      var res = await _driver.QueryAsync(GetSelectQuery());
      return await res.GetSingleOrDefault();
    }

    public async Task<List<TNewModel>> ToListAsync()
    {
      var res = await _driver.QueryAsync(GetSelectQuery());
      return await res.GetAll();
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      return GetSelectQuery().GetRawQuery(defaultSchema);
    }
  }

  public class SelectQueryFluentBuilder3<TModel, TNewModel> { }
}