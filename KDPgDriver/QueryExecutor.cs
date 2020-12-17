using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using KDPgDriver.Fluent;
using KDPgDriver.Queries;
using KDPgDriver.Results;

namespace KDPgDriver
{
  public abstract class QueryExecutor
  {
    public abstract void ScheduleQuery(IQuery query);
    public abstract Task<InsertQueryResult> QueryAsync(IInsertQuery insertQuery);
    public abstract Task<UpdateQueryResult> QueryAsync(IUpdateQuery updateQuery);
    public abstract Task<SelectQueryResult<TOut>> QueryAsync<TModel, TOut>(SelectQuery<TModel, TOut> selectQuery);
    public abstract Task<DeleteQueryResult> QueryAsync(IDeleteQuery updateQuery);

    public SelectQueryFluentBuilder1Prep<TModel> From<TModel>() => new SelectQueryFluentBuilder1Prep<TModel>(this);
    public InsertQueryFluentBuilder1<TModel> Insert<TModel>() => new InsertQueryFluentBuilder1<TModel>(this);
    public UpdateQueryFluentBuilder1<TModel> Update<TModel>() => new UpdateQueryFluentBuilder1<TModel>(this);
    public DeleteQueryFluentBuilder1<TModel> Delete<TModel>() => new DeleteQueryFluentBuilder1<TModel>(this);

    public InsertQueryFluentBuilder1<T> Insert<T>(T obj)
    {
      var builder = new InsertQueryFluentBuilder1<T>(this);
      builder.AddObject(obj);
      return builder;
    }

    public InsertQueryFluentBuilder1<T> InsertMany<T>(IEnumerable<T> objects)
    {
      var builder = new InsertQueryFluentBuilder1<T>(this);
      builder.AddMany(objects);
      return builder;
    }

    public SelectMultipleQueryFluentBuilderPrep2<TModel1, TModel2> FromMany<TModel1, TModel2>(
        Expression<Func<TModel1, TModel2, bool>> joinCondition)
    {
      return new SelectMultipleQueryFluentBuilderPrep2<TModel1, TModel2>(this, joinCondition);
    }

    public SelectMultipleQueryFluentBuilderPrep3<TModel1, TModel2, TModel3> FromMany<TModel1, TModel2, TModel3>(
        Expression<Func<TModel1, TModel2, bool>> joinCondition1,
        Expression<Func<TModel1, TModel2, TModel3, bool>> joinCondition2)
    {
      return new SelectMultipleQueryFluentBuilderPrep3<TModel1, TModel2, TModel3>(this, joinCondition1, joinCondition2);
    }

    public SelectMultipleQueryFluentBuilderPrep4<TModel1, TModel2, TModel3, TModel4> FromMany<TModel1, TModel2, TModel3, TModel4>(
        Expression<Func<TModel1, TModel2, bool>> joinCondition1,
        Expression<Func<TModel1, TModel2, TModel3, bool>> joinCondition2,
        Expression<Func<TModel1, TModel2, TModel3, TModel4, bool>> joinCondition3)
    {
      return new SelectMultipleQueryFluentBuilderPrep4<TModel1, TModel2, TModel3, TModel4>(this, joinCondition1, joinCondition2, joinCondition3);
    }
  }
}