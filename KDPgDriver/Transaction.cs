using System;
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

    public Task<SelectQueryResult<TOut>> QueryAsync<TOut>(SelectQuery<TOut> builder)
      => Driver.QueryAsyncInternal(builder, NpgsqlConnection, NpgsqlTransaction);

    public Task<InsertQueryResult> QueryAsync<TOut>(InsertQuery<TOut> builder)
      => Driver.QueryAsyncInternal(builder, NpgsqlConnection, NpgsqlTransaction);

    public Task<UpdateQueryResult> QueryAsync<TOut>(UpdateQuery<TOut> builder)
      => Driver.QueryAsyncInternal(builder, NpgsqlConnection, NpgsqlTransaction);

    public Task<DeleteQueryResult> QueryAsync(DeleteQuery builder)
      => Driver.QueryAsyncInternal(builder, NpgsqlConnection, NpgsqlTransaction);

    // Chains
    public SelectQueryFluentBuilder1<TModel> From<TModel>()
    {
      return new SelectQueryFluentBuilder1<TModel>(this);
    }
  }
}