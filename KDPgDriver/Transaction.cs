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

    public override void ScheduleQuery(IQuery query)
    {
      throw new Exception("Schedule works only for batch query");
    }

    public override async Task<SelectQueryResult<TOut>> QueryAsync<TModel, TOut>(SelectQuery<TModel, TOut> query)
    {
      var b = Batch.CreateUsingTransaction(this);
      var res = b.QueryAsync(query);
      await b.Execute();
      return res.Result;
    }

    public override async Task<InsertQueryResult> QueryAsync(IInsertQuery query)
    {
      var b = Batch.CreateUsingTransaction(this);
      var res = b.QueryAsync(query);
      await b.Execute();
      return res.Result;
    }

    public override async Task<UpdateQueryResult> QueryAsync(IUpdateQuery query)
    {
      var b = Batch.CreateUsingTransaction(this);
      var res = b.QueryAsync(query);
      await b.Execute();
      return res.Result;
    }

    public override async Task<DeleteQueryResult> QueryAsync(IDeleteQuery query)
    {
      var b = Batch.CreateUsingTransaction(this);
      var res = b.QueryAsync(query);
      await b.Execute();
      return res.Result;
    }
  }
}