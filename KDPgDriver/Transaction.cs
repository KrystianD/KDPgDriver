using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using KDPgDriver.Fluent;
using KDPgDriver.Queries;
using KDPgDriver.Results;
using Npgsql;

namespace KDPgDriver
{
  public class Transaction : IQueryExecutor, IDisposable
  {
    public Driver Driver { get; }

    internal NpgsqlConnection NpgsqlConnection { get; }
    internal NpgsqlTransaction NpgsqlTransaction { get; }

    public Transaction(Driver driver, NpgsqlConnection connection, NpgsqlTransaction transaction)
    {
      Driver = driver;
      NpgsqlConnection = connection;
      NpgsqlTransaction = transaction;
    }

    public void Dispose()
    {
      NpgsqlConnection.Close();
      NpgsqlConnection.Dispose();
      NpgsqlTransaction.Dispose();
    }

    public Batch CreateBatch() => Batch.CreateUsingTransaction(this);

    public Task CommitAsync() => NpgsqlTransaction.CommitAsync();

    public void ScheduleQuery(IQuery query)
    {
      throw new Exception("Schedule works only for batch query");
    }

    public async Task<SelectQueryResult<TOut>> QueryAsync<TModel, TOut>(SelectQuery<TModel, TOut> query)
    {
      var b = Batch.CreateUsingTransaction(this);
      var res = b.QueryAsync(query);
      await b.Execute();
      return res.Result;
    }

    public async Task<InsertQueryResult> QueryAsync(IInsertQuery query)
    {
      var b = Batch.CreateUsingTransaction(this);
      var res = b.QueryAsync(query);
      await b.Execute();
      return res.Result;
    }

    public async Task<UpdateQueryResult> QueryAsync(IUpdateQuery query)
    {
      var b = Batch.CreateUsingTransaction(this);
      var res = b.QueryAsync(query);
      await b.Execute();
      return res.Result;
    }

    public async Task<DeleteQueryResult> QueryAsync(IDeleteQuery query)
    {
      var b = Batch.CreateUsingTransaction(this);
      var res = b.QueryAsync(query);
      await b.Execute();
      return res.Result;
    }

    // Chains
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