using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using KDPgDriver.Fluent;
using KDPgDriver.Queries;
using KDPgDriver.Results;
using KDPgDriver.Utils;
using Npgsql;

namespace KDPgDriver
{
  public class Batch : IQueryExecutor
  {
    private readonly Driver _driver;
    private readonly Transaction _transaction;

    private interface IOperation
    {
      Task Process(NpgsqlDataReader r);
    }

    private class Operation<T> : IOperation
    {
      public Func<NpgsqlDataReader, Task> ResultProcessorFunc;
      public readonly TaskCompletionSource<T> TaskCompletionSource = new TaskCompletionSource<T>();

      public Task Process(NpgsqlDataReader r) => ResultProcessorFunc(r);
    }

    public Batch(Driver driver)
    {
      _driver = driver;
      _transaction = null;
    }

    public Batch(Transaction transaction)
    {
      _driver = transaction.Driver;
      _transaction = transaction;
    }

    private readonly RawQuery _combinedRawQuery = new RawQuery();
    private readonly List<IOperation> _operations = new List<IOperation>();

    public Task<SelectQueryResult<TOut>> QueryAsync<TModel, TOut>(SelectQuery<TModel, TOut> builder)
    {
      _combinedRawQuery.Append(builder.GetRawQuery(_driver.Schema));
      _combinedRawQuery.Append("; ");

      var op = new Operation<SelectQueryResult<TOut>>();
      op.ResultProcessorFunc = async reader =>
      {
        var res = new SelectQueryResult<TOut>(builder);
        await res.ProcessResultSet(reader);

        op.TaskCompletionSource.SetResult(res);
      };
      _operations.Add(op);

      return op.TaskCompletionSource.Task;
    }

    public Task<InsertQueryResult> QueryAsync(IInsertQuery builder)
    {
      _combinedRawQuery.Append(builder.GetRawQuery(_driver.Schema));
      _combinedRawQuery.Append("; ");

      var op = new Operation<InsertQueryResult>();
      op.ResultProcessorFunc = async reader =>
      {
        await reader.ReadAsync();
        var lastInsertId = reader.GetInt32(0);
        var res = new InsertQueryResult(lastInsertId);

        op.TaskCompletionSource.SetResult(res);
      };
      _operations.Add(op);

      return op.TaskCompletionSource.Task;
    }

    public Task<UpdateQueryResult> QueryAsync(IUpdateQuery builder)
    {
      _combinedRawQuery.Append(builder.GetRawQuery(_driver.Schema));
      _combinedRawQuery.Append("; ");

      var op = new Operation<UpdateQueryResult>();
      op.ResultProcessorFunc = async reader =>
      {
        var res = new UpdateQueryResult();
        op.TaskCompletionSource.SetResult(res);
      };
      _operations.Add(op);

      return op.TaskCompletionSource.Task;
    }

    public Task<DeleteQueryResult> QueryAsync(IDeleteQuery builder)
    {
      _combinedRawQuery.Append(builder.GetRawQuery(_driver.Schema));
      _combinedRawQuery.Append("; ");

      var op = new Operation<DeleteQueryResult>();
      op.ResultProcessorFunc = async reader =>
      {
        var res = new DeleteQueryResult();
        op.TaskCompletionSource.SetResult(res);
      };
      _operations.Add(op);

      return op.TaskCompletionSource.Task;
    }

    public async Task Execute()
    {
      string query;
      ParametersContainer parameters;
      _combinedRawQuery.Render(out query, out parameters);

      Console.WriteLine(query);

      async Task DoOperation(NpgsqlConnection conn, NpgsqlTransaction tran)
      {
        using (var cmd = new NpgsqlCommand(query, conn, tran)) {
          parameters.AssignToCommand(cmd);

          using (var reader = await cmd.ExecuteReaderAsync()) {
            foreach (var operation in _operations) {
              await operation.Process((NpgsqlDataReader) reader);
              await reader.NextResultAsync();
            }
          }
        }
      }

      if (_transaction != null)
        await DoOperation(_transaction.NpgsqlConnection, _transaction.NpgsqlTransaction);
      else
        using (var connection = await _driver.CreateConnection())
          await DoOperation(connection, null);
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
  }
}