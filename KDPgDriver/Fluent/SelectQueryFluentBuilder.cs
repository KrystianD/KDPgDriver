using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using KDLib;
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
      return new SelectQueryFluentBuilder2<TModel, TModel>(SelectFromBuilder.AllColumns<TModel>(), _executor);
    }

    public SelectQueryFluentBuilder2<TModel, TNewModel> Select<TNewModel>(Expression<Func<TModel, TNewModel>> pr)
    {
      return new SelectQueryFluentBuilder2<TModel, TNewModel>(SelectFromBuilder.FromExpression(pr), _executor);
    }

    public SelectQueryFluentBuilder2<TModel, TModel> SelectOnly(FieldListBuilder<TModel> builder)
    {
      return new SelectQueryFluentBuilder2<TModel, TModel>(SelectFromBuilder.FromFieldListBuilder(builder), _executor);
    }

    public SelectQueryFluentBuilder2<TModel, TModel> SelectOnly(params Expression<Func<TModel, object>>[] fieldsList)
    {
      var builder = new FieldListBuilder<TModel>();
      foreach (var expression in fieldsList)
        builder.AddField(expression);
      return SelectOnly(builder);
    }
  }

  public class SelectQueryFluentBuilder2<TModel, TNewModel> : IQuery
  {
    private readonly IQueryExecutor _executor;
    private readonly SelectFromBuilder _selectFromBuilder;
    private readonly OrderBuilder<TModel> _orderBuilder = new OrderBuilder<TModel>();
    private readonly WhereBuilder<TModel> _whereBuilder = WhereBuilder<TModel>.Empty;
    private readonly LimitBuilder _limitBuilder = new LimitBuilder();

    public SelectQueryFluentBuilder2(SelectFromBuilder selectFromBuilder, IQueryExecutor executor = null)
    {
      _executor = executor;
      _selectFromBuilder = selectFromBuilder;
    }

    public SelectQueryFluentBuilder2<TModel, TNewModel> Where(Expression<Func<TModel, bool>> exp)
    {
      _whereBuilder.AndWith(WhereBuilder<TModel>.FromExpression(exp));
      return this;
    }

    public SelectQueryFluentBuilder2<TModel, TNewModel> Where(WhereBuilder<TModel> builder)
    {
      _whereBuilder.AndWith(builder);
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

    public SelectQuery<TModel, TNewModel> GetSelectQuery()
    {
      return new SelectQuery<TModel, TNewModel>(_whereBuilder, _selectFromBuilder, _orderBuilder, _limitBuilder);
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

    public async Task<Dictionary<T, TNewModel>> ToDictionaryAsync<T>(Func<TNewModel, T> keySelector)
    {
      return (await ToListAsync()).ToDictionary(keySelector);
    }

    public async Task<Dictionary<T, V>> ToDictionaryAsync<T, V>(Func<TNewModel, T> keySelector, Func<TNewModel, V> elementSelector)
    {
      return (await ToListAsync()).ToDictionary(keySelector, elementSelector);
    }

    public async Task<Dictionary<T, List<TNewModel>>> ToDictionaryGroupAsync<T>(Func<TNewModel, T> keySelector)
    {
      return (await ToListAsync()).GroupByToDictionary(keySelector);
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      return GetSelectQuery().GetRawQuery(defaultSchema);
    }
  }
}