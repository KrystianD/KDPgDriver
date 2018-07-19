using System;
using System.Collections.Generic;
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

    public Batch CreateBatch() => new Batch(this);

    public Task CommitAsync() => NpgsqlTransaction.CommitAsync();

    public Task<SelectQueryResult<TOut>> QueryAsync<TModel, TOut>(SelectQuery<TModel, TOut> builder)
      => Driver.QueryAsyncInternal(builder, NpgsqlConnection, NpgsqlTransaction);

    public Task<InsertQueryResult> QueryAsync(IInsertQuery builder)
      => Driver.QueryAsyncInternal(builder, NpgsqlConnection, NpgsqlTransaction);

    public Task<UpdateQueryResult> QueryAsync(IUpdateQuery builder)
      => Driver.QueryAsyncInternal(builder, NpgsqlConnection, NpgsqlTransaction);

    public Task<DeleteQueryResult> QueryAsync(IDeleteQuery builder)
      => Driver.QueryAsyncInternal(builder, NpgsqlConnection, NpgsqlTransaction);

    // Chains
    public SelectQueryFluentBuilder1<TModel> From<TModel>() => new SelectQueryFluentBuilder1<TModel>(this);
    public InsertQueryFluentBuilder1<TModel> Insert<TModel>() => new InsertQueryFluentBuilder1<TModel>(this);
    public UpdateQueryFluentBuilder1<TModel> Update<TModel>() => new UpdateQueryFluentBuilder1<TModel>(this);
    public DeleteQueryFluentBuilder1<TModel> Delete<TModel>() => new DeleteQueryFluentBuilder1<TModel>(this);
    
    public InsertQueryFluentBuilder1<T> Insert<T>(T obj)
    {
      var builder = new InsertQueryFluentBuilder1<T>(this);
      builder.AddObject(obj);
      return builder;
    }

    public InsertQueryFluentBuilder1<T> Insert<T>(IEnumerable<T> objects)
    {
      var builder = new InsertQueryFluentBuilder1<T>(this);
      builder.AddMany(objects);
      return builder;
    }
  }
}