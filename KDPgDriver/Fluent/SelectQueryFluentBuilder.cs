using System;
using System.Collections.Generic;
using System.Data;
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
  public class TablesList
  {
    public List<TypedExpression> JoinExpressions = new List<TypedExpression>();

    // public List<KdPgTableDescriptor> Tables { get; } = new List<KdPgTableDescriptor>();
    public List<RawQuery.TableNamePlaceholder> Tables { get; } = new List<RawQuery.TableNamePlaceholder>();

    public void AddModel<T>(RawQuery.TableNamePlaceholder tableName, TypedExpression joinExpression = null)
    {
      Tables.Add(tableName);
      JoinExpressions.Add(joinExpression);
    }
  }

  public class SelectQueryFluentBuilder1Prep<TModel>
  {
    private readonly IQueryExecutor _executor;

    public SelectQueryFluentBuilder1Prep()
    {
    }

    public SelectQueryFluentBuilder1Prep(IQueryExecutor executor)
    {
      _executor = executor;
    }

    public SelectQueryFluentBuilder<TModel, TModel> Select()
    {
      return new SelectQueryFluentBuilder<TModel, TModel>(SelectFromBuilder.AllColumns<TModel>(), _executor);
    }

    public SelectQueryFluentBuilder<TModel, TNewModel> Select<TNewModel>(Expression<Func<TModel, TNewModel>> pr)
    {
      return new SelectQueryFluentBuilder<TModel, TNewModel>(SelectFromBuilder.FromExpression(pr), _executor);
    }

    public SelectQueryFluentBuilder<TModel, TModel> SelectOnly(FieldListBuilder<TModel> builder)
    {
      return new SelectQueryFluentBuilder<TModel, TModel>(SelectFromBuilder.FromFieldListBuilder(builder), _executor);
    }

    public SelectQueryFluentBuilder<TModel, TModel> SelectOnly(params Expression<Func<TModel, object>>[] fieldsList)
    {
      var builder = new FieldListBuilder<TModel>();
      foreach (var expression in fieldsList)
        builder.AddField(expression);
      return SelectOnly(builder);
    }
  }

  public class BaseSelectMultipleQueryFluentBuilderPrep
  {
  }

  public class SelectMultipleQueryFluentBuilderPrep2<TModel1, TModel2> : BaseSelectMultipleQueryFluentBuilderPrep
  {
    private readonly IQueryExecutor _executor;
    private readonly TypedExpression _joinCondition1;

    private readonly RawQuery.TableNamePlaceholder _p1 = new RawQuery.TableNamePlaceholder(ModelsRegistry.GetTable<TModel1>(), null);
    private readonly RawQuery.TableNamePlaceholder _p2 = new RawQuery.TableNamePlaceholder(ModelsRegistry.GetTable<TModel2>(), null);

    public SelectMultipleQueryFluentBuilderPrep2(IQueryExecutor executor,
                                                 Expression<Func<TModel1, TModel2, bool>> joinCondition)
    {
      var options = new NodeVisitor.EvaluationOptions();
      options.ParameterToTableAlias.Add(joinCondition.Parameters[0], _p1);
      options.ParameterToTableAlias.Add(joinCondition.Parameters[1], _p2);

      _joinCondition1 = NodeVisitor.VisitFuncExpression(joinCondition, options);
      _executor = executor;
    }

    public SelectMultipleQueryFluentBuilderMapper<TCombinedModel> Map<TCombinedModel>(Expression<Func<TModel1, TModel2, TCombinedModel>> pr)
    {
      var argsMap = ((NewExpression) pr.Body).Members.Zip(((NewExpression) pr.Body).Arguments).ToDictionary(x => x.Item2, x => x.Item1.Name);
      var inpParams = pr.Parameters.Select(x => argsMap.GetValueOrDefault(x, x.Name)).ToList();

      _p1.Name = inpParams[0];
      _p2.Name = inpParams[1];

      if (pr == null) throw new ArgumentNullException(nameof(pr));
      TablesList tl = new TablesList();
      tl.AddModel<TModel1>(_p1);
      tl.AddModel<TModel2>(_p2, _joinCondition1);
      return new SelectMultipleQueryFluentBuilderMapper<TCombinedModel>(_executor, tl);
    }
  }

  public class SelectMultipleQueryFluentBuilderPrep3<TModel1, TModel2, TModel3> : BaseSelectMultipleQueryFluentBuilderPrep
  {
    private readonly IQueryExecutor _executor;
    private readonly TypedExpression _joinCondition1;
    private readonly TypedExpression _joinCondition2;

    private readonly RawQuery.TableNamePlaceholder _p1 = new RawQuery.TableNamePlaceholder(ModelsRegistry.GetTable<TModel1>(), null);
    private readonly RawQuery.TableNamePlaceholder _p2 = new RawQuery.TableNamePlaceholder(ModelsRegistry.GetTable<TModel2>(), null);
    private readonly RawQuery.TableNamePlaceholder _p3 = new RawQuery.TableNamePlaceholder(ModelsRegistry.GetTable<TModel3>(), null);

    public SelectMultipleQueryFluentBuilderPrep3(IQueryExecutor executor,
                                                 Expression<Func<TModel1, TModel2, bool>> joinCondition1,
                                                 Expression<Func<TModel1, TModel2, TModel3, bool>> joinCondition2)
    {
      var options = new NodeVisitor.EvaluationOptions();
      options.ParameterToTableAlias.Add(joinCondition1.Parameters[0], _p1);
      options.ParameterToTableAlias.Add(joinCondition1.Parameters[1], _p2);

      _joinCondition1 = NodeVisitor.VisitFuncExpression(joinCondition1, options);

      options = new NodeVisitor.EvaluationOptions();
      options.ParameterToTableAlias.Add(joinCondition2.Parameters[0], _p1);
      options.ParameterToTableAlias.Add(joinCondition2.Parameters[1], _p2);
      options.ParameterToTableAlias.Add(joinCondition2.Parameters[2], _p3);

      _joinCondition2 = NodeVisitor.VisitFuncExpression(joinCondition2, options);
      _executor = executor;
    }

    public SelectMultipleQueryFluentBuilderMapper<TCombinedModel> Map<TCombinedModel>(Expression<Func<TModel1, TModel2, TModel3, TCombinedModel>> pr)
    {
      var argsMap = ((NewExpression) pr.Body).Members.Zip(((NewExpression) pr.Body).Arguments).ToDictionary(x => x.Item2, x => x.Item1.Name);
      var inpParams = pr.Parameters.Select(x => argsMap.GetValueOrDefault(x, x.Name)).ToList();

      _p1.Name = inpParams[0];
      _p2.Name = inpParams[1];
      _p3.Name = inpParams[2];

      if (pr == null) throw new ArgumentNullException(nameof(pr));
      TablesList tl = new TablesList();
      tl.AddModel<TModel1>(_p1);
      tl.AddModel<TModel2>(_p2, _joinCondition1);
      tl.AddModel<TModel3>(_p3, _joinCondition2);
      return new SelectMultipleQueryFluentBuilderMapper<TCombinedModel>(_executor, tl);
    }
  }

  public class SelectMultipleQueryFluentBuilderPrep4<TModel1, TModel2, TModel3, TModel4> : BaseSelectMultipleQueryFluentBuilderPrep
  {
    private readonly IQueryExecutor _executor;
    private readonly TypedExpression _joinCondition1;
    private readonly TypedExpression _joinCondition2;
    private readonly TypedExpression _joinCondition3;

    private readonly RawQuery.TableNamePlaceholder _p1 = new RawQuery.TableNamePlaceholder(ModelsRegistry.GetTable<TModel1>(), null);
    private readonly RawQuery.TableNamePlaceholder _p2 = new RawQuery.TableNamePlaceholder(ModelsRegistry.GetTable<TModel2>(), null);
    private readonly RawQuery.TableNamePlaceholder _p3 = new RawQuery.TableNamePlaceholder(ModelsRegistry.GetTable<TModel3>(), null);
    private readonly RawQuery.TableNamePlaceholder _p4 = new RawQuery.TableNamePlaceholder(ModelsRegistry.GetTable<TModel4>(), null);

    public SelectMultipleQueryFluentBuilderPrep4(IQueryExecutor executor,
                                                 Expression<Func<TModel1, TModel2, bool>> joinCondition1,
                                                 Expression<Func<TModel1, TModel2, TModel3, bool>> joinCondition2,
                                                 Expression<Func<TModel1, TModel2, TModel3, TModel4, bool>> joinCondition3)
    {
      var options = new NodeVisitor.EvaluationOptions();
      options.ParameterToTableAlias.Add(joinCondition1.Parameters[0], _p1);
      options.ParameterToTableAlias.Add(joinCondition1.Parameters[1], _p2);

      _joinCondition1 = NodeVisitor.VisitFuncExpression(joinCondition1, options);

      options = new NodeVisitor.EvaluationOptions();
      options.ParameterToTableAlias.Add(joinCondition2.Parameters[0], _p1);
      options.ParameterToTableAlias.Add(joinCondition2.Parameters[1], _p2);
      options.ParameterToTableAlias.Add(joinCondition2.Parameters[2], _p3);

      _joinCondition2 = NodeVisitor.VisitFuncExpression(joinCondition2, options);

      options = new NodeVisitor.EvaluationOptions();
      options.ParameterToTableAlias.Add(joinCondition3.Parameters[0], _p1);
      options.ParameterToTableAlias.Add(joinCondition3.Parameters[1], _p2);
      options.ParameterToTableAlias.Add(joinCondition3.Parameters[2], _p3);
      options.ParameterToTableAlias.Add(joinCondition3.Parameters[3], _p4);

      _joinCondition3 = NodeVisitor.VisitFuncExpression(joinCondition3, options);

      _executor = executor;
    }

    public SelectMultipleQueryFluentBuilderMapper<TCombinedModel> Map<TCombinedModel>(Expression<Func<TModel1, TModel2, TModel3, TModel4, TCombinedModel>> pr)
    {
      var argsMap = ((NewExpression) pr.Body).Members.Zip(((NewExpression) pr.Body).Arguments).ToDictionary(x => x.Item2, x => x.Item1.Name);
      var inpParams = pr.Parameters.Select(x => argsMap.GetValueOrDefault(x, x.Name)).ToList();

      _p1.Name = inpParams[0];
      _p2.Name = inpParams[1];
      _p3.Name = inpParams[2];
      _p4.Name = inpParams[3];

      if (pr == null) throw new ArgumentNullException(nameof(pr));
      TablesList tl = new TablesList();
      tl.AddModel<TModel1>(_p1);
      tl.AddModel<TModel2>(_p2, _joinCondition1);
      tl.AddModel<TModel3>(_p3, _joinCondition2);
      tl.AddModel<TModel3>(_p4, _joinCondition3);
      return new SelectMultipleQueryFluentBuilderMapper<TCombinedModel>(_executor, tl);
    }
  }

  // public class SelectMultipleQueryFluentBuilderPrep4<TModel1, TModel2, TModel3, TModel4> : BaseSelectMultipleQueryFluentBuilderPrep
  // {
  //   private readonly IQueryExecutor _executor;
  //
  //   public SelectMultipleQueryFluentBuilderPrep4(IQueryExecutor executor)
  //   {
  //     _executor = executor;
  //   }
  //
  //   public SelectMultipleQueryFluentBuilderMapper<TCombinedModel> Map<TCombinedModel>(Expression<Func<TModel1, TModel2, TModel3, TModel4, TCombinedModel>> pr)
  //   {
  //     if (pr == null) throw new ArgumentNullException(nameof(pr));
  //     TablesList tl = new TablesList();
  //     tl.AddModel<TModel1>();
  //     tl.AddModel<TModel2>();
  //     tl.AddModel<TModel3>();
  //     tl.AddModel<TModel4>();
  //     // tl.leftJoins = leftJoins;
  //     return new SelectMultipleQueryFluentBuilderMapper<TCombinedModel>(_executor, tl);
  //   }
  // }
  //
  // public class SelectMultipleQueryFluentBuilderPrep5<TModel1, TModel2, TModel3, TModel4, TModel5> : BaseSelectMultipleQueryFluentBuilderPrep
  // {
  //   private readonly IQueryExecutor _executor;
  //
  //   public SelectMultipleQueryFluentBuilderPrep5(IQueryExecutor executor)
  //   {
  //     _executor = executor;
  //   }
  //
  //   // public new SelectMultipleQueryFluentBuilderPrep5<TModel1, TModel2, TModel3, TModel4, TModel5> ConfigureLeftJoin<T1, T2>(Expression<Func<T1, T2, bool>> joinCondition)
  //   // {
  //   //   base.ConfigureLeftJoin(joinCondition);
  //   //   return this;
  //   // }
  //
  //   public SelectMultipleQueryFluentBuilderMapper<TCombinedModel> Map<TCombinedModel>(Expression<Func<TModel1, TModel2, TModel3, TModel4, TModel5, TCombinedModel>> pr)
  //   {
  //     if (pr == null) throw new ArgumentNullException(nameof(pr));
  //     TablesList tl = new TablesList();
  //     tl.AddModel<TModel1>();
  //     tl.AddModel<TModel2>();
  //     tl.AddModel<TModel3>();
  //     tl.AddModel<TModel4>();
  //     tl.AddModel<TModel5>();
  //     // tl.leftJoins = leftJoins;
  //     return new SelectMultipleQueryFluentBuilderMapper<TCombinedModel>(_executor, tl);
  //   }
  // }

  public class SelectMultipleQueryFluentBuilderMapper<TCombinedModel>
  {
    private readonly TablesList _tablesList;
    private readonly IQueryExecutor _executor;


    public SelectMultipleQueryFluentBuilderMapper(IQueryExecutor executor, TablesList tablesList)
    {
      _executor = executor;
      _tablesList = tablesList;
    }

    public SelectQueryFluentBuilder<TCombinedModel, TCombinedModel> Select()
    {
      return new SelectQueryFluentBuilder<TCombinedModel, TCombinedModel>(SelectFromBuilder.AllColumnsFromCombined<TCombinedModel>(_tablesList), _executor);
    }

    public SelectQueryFluentBuilder<TCombinedModel, TNewModel> Select<TNewModel>(Expression<Func<TCombinedModel, TNewModel>> pr)
    {
      return new SelectQueryFluentBuilder<TCombinedModel, TNewModel>(SelectFromBuilder.FromCombinedExpression(_tablesList, pr), _executor);
    }
  }

  public class SelectQueryFluentBuilder<TModel, TNewModel> : IQuery
  {
    private readonly IQueryExecutor _executor;
    private readonly SelectFromBuilder _selectFromBuilder;
    private readonly OrderBuilder<TModel> _orderBuilder = new OrderBuilder<TModel>();
    private readonly WhereBuilder<TModel> _whereBuilder = WhereBuilder<TModel>.Empty;
    private readonly LimitBuilder _limitBuilder = new LimitBuilder();

    public SelectQueryFluentBuilder(SelectFromBuilder selectFromBuilder, IQueryExecutor executor = null)
    {
      _executor = executor;
      _selectFromBuilder = selectFromBuilder;
    }

    public SelectQueryFluentBuilder<TModel, TNewModel> Where(Expression<Func<TModel, bool>> exp)
    {
      _whereBuilder.AndWith(WhereBuilder<TModel>.FromExpression(exp));
      return this;
    }

    public SelectQueryFluentBuilder<TModel, TNewModel> Where(WhereBuilder<TModel> builder)
    {
      _whereBuilder.AndWith(builder);
      return this;
    }

    public SelectQueryFluentBuilder<TModel, TNewModel> OrderBy(Expression<Func<TModel, object>> exp)
    {
      _orderBuilder.OrderBy(exp);
      return this;
    }

    public SelectQueryFluentBuilder<TModel, TNewModel> OrderByDescending(Expression<Func<TModel, object>> exp)
    {
      _orderBuilder.OrderByDescending(exp);
      return this;
    }

    public SelectQueryFluentBuilder<TModel, TNewModel> Limit(int limit)
    {
      _limitBuilder.Limit(limit);
      return this;
    }

    public SelectQueryFluentBuilder<TModel, TNewModel> Offset(int offset)
    {
      _limitBuilder.Offset(offset);
      return this;
    }

    public SelectQuery<TModel, TNewModel> GetSelectQuery()
    {
      return new SelectQuery<TModel, TNewModel>(_whereBuilder, _selectFromBuilder, _orderBuilder, _limitBuilder);
    }

    public SelectSubquery<TNewModel> AsScalarSubquery()
    {
      return new SelectSubquery<TNewModel>(GetSelectQuery());
    }

    public SelectSubquery<IList<TNewModel>> AsListSubquery()
    {
      return new SelectSubquery<IList<TNewModel>>(GetSelectQuery());
    }

    public async Task<TNewModel> ToSingleAsync()
    {
      var res = await _executor.QueryAsync(GetSelectQuery());
      return res.GetSingle();
    }

    public async Task<TNewModel> ToSingleOrDefaultAsync(TNewModel def = default)
    {
      var res = await _executor.QueryAsync(GetSelectQuery());
      return res.GetSingleOrDefault(def);
    }

    public async Task<List<TNewModel>> ToListAsync()
    {
      var res = await _executor.QueryAsync(GetSelectQuery());
      return res.GetAll();
    }

    public async Task<List<T>> ToListAsync<T>(Func<TNewModel, T> project)
    {
      return (await ToListAsync()).Select(project).ToList();
    }

    public async Task<HashSet<TNewModel>> ToHashSetAsync()
    {
      return (await ToListAsync()).ToHashSet();
    }

    public async Task<HashSet<T>> ToHashSetAsync<T>(Func<TNewModel, T> elementSelector)
    {
      return (await ToListAsync()).Select(elementSelector).ToHashSet();
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

    public async Task<Dictionary<T, List<V>>> ToDictionaryGroupAsync<T, V>(Func<TNewModel, T> keySelector, Func<TNewModel, V> elementSelector)
    {
      return (await ToListAsync()).GroupByToDictionary(keySelector, elementSelector);
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      return GetSelectQuery().GetRawQuery(defaultSchema);
    }
  }
}