using System;
using System.Threading.Tasks;
using KDPgDriver.Queries;
using KDPgDriver.Results;
using Npgsql;

namespace KDPgDriver
{
  public class Transaction : QueryExecutor, IDisposable
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

    public override async Task<SelectQueryResult<TOut>> QueryAsync<TModel, TOut>(SelectQuery<TModel, TOut> selectQuery)
    {
      var b = Batch.CreateUsingTransaction(this);
      var res = b.QueryAsync(selectQuery);
      await b.Execute();
      return res.Result;
    }

    public override async Task<InsertQueryResult> QueryAsync(IInsertQuery insertQuery)
    {
      var b = Batch.CreateUsingTransaction(this);
      var res = b.QueryAsync(insertQuery);
      await b.Execute();
      return res.Result;
    }

    public override async Task<UpdateQueryResult> QueryAsync(IUpdateQuery updateQuery)
    {
      var b = Batch.CreateUsingTransaction(this);
      var res = b.QueryAsync(updateQuery);
      await b.Execute();
      return res.Result;
    }

    public override async Task<DeleteQueryResult> QueryAsync(IDeleteQuery deleteQuery)
    {
      var b = Batch.CreateUsingTransaction(this);
      var res = b.QueryAsync(deleteQuery);
      await b.Execute();
      return res.Result;
    }
  }
}