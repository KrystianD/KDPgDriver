using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using KDPgDriver.Queries;
using KDPgDriver.Results;
using KDPgDriver.Utils;
using Npgsql;

namespace KDPgDriver
{
  public class Batch : QueryExecutor
  {
    private delegate Task ResultProcessorHandler(NpgsqlDataReader reader);

    private readonly RawQuery _combinedRawQuery = new RawQuery();
    private readonly List<ResultProcessorHandler> _resultProcessors = new List<ResultProcessorHandler>();
    private readonly Func<Func<NpgsqlConnection, NpgsqlTransaction, Task>, Task> _connectionCreator;

    [PublicAPI]
    public bool IsEmpty { get; private set; } = true;

    private Batch(Func<Func<NpgsqlConnection, NpgsqlTransaction, Task>, Task> connectionCreator)
    {
      _connectionCreator = connectionCreator;
    }

    public override void ScheduleQuery(IQuery query)
    {
      _combinedRawQuery.Append(query.GetRawQuery());
      _combinedRawQuery.Append(";\n");
      IsEmpty = false;
    }

    public override Task<SelectQueryResult<TOut>> QueryAsync<TModel, TOut>(SelectQuery<TModel, TOut> builder)
    {
      _combinedRawQuery.Append(builder.GetRawQuery());
      _combinedRawQuery.Append(";\n");
      IsEmpty = false;

      var tcs = new TaskCompletionSource<SelectQueryResult<TOut>>();

      _resultProcessors.Add(async reader => {
        var res = new SelectQueryResult<TOut>(builder);
        await res.ProcessResultSet(reader);

        tcs.SetResult(res);
      });

      return tcs.Task;
    }

    public override Task<InsertQueryResult> QueryAsync(IInsertQuery builder)
    {
      _combinedRawQuery.Append(builder.GetRawQuery());
      _combinedRawQuery.Append(";\n");
      IsEmpty = false;

      var tcs = new TaskCompletionSource<InsertQueryResult>();

      _resultProcessors.Add(async reader => {
        InsertQueryResult res;
        if (await reader.ReadAsync()) {
          var lastInsertId = reader.GetInt32(0);
          res = InsertQueryResult.CreateRowInserted(lastInsertId);
        }
        else {
          res = InsertQueryResult.CreateRowNotInserted();
        }

        tcs.SetResult(res);
      });

      return tcs.Task;
    }

    public override Task<UpdateQueryResult> QueryAsync(IUpdateQuery builder)
    {
      _combinedRawQuery.Append(builder.GetRawQuery());
      _combinedRawQuery.Append(";\n");
      IsEmpty = false;

      var tcs = new TaskCompletionSource<UpdateQueryResult>();

      _resultProcessors.Add(reader => {
        var res = new UpdateQueryResult();
        tcs.SetResult(res);
        return Task.CompletedTask;
      });

      return tcs.Task;
    }

    public override Task<DeleteQueryResult> QueryAsync(IDeleteQuery builder)
    {
      _combinedRawQuery.Append(builder.GetRawQuery());
      _combinedRawQuery.Append(";\n");
      IsEmpty = false;

      var tcs = new TaskCompletionSource<DeleteQueryResult>();

      _resultProcessors.Add(reader => {
        var res = new DeleteQueryResult();
        tcs.SetResult(res);
        return Task.CompletedTask;
      });

      return tcs.Task;
    }

    public async Task Execute()
    {
      string query;
      ParametersContainer parameters;

      if (IsEmpty)
        return;

      _combinedRawQuery.Render(out query, out parameters);

      Console.WriteLine(query);

      await _connectionCreator(async (connection, transaction) => {
        using var cmd = new NpgsqlCommand(query, connection, transaction);

        parameters.AssignToCommand(cmd);

        using var reader = await cmd.ExecuteReaderAsync();

        foreach (var operation in _resultProcessors) {
          await operation(reader);
          await reader.NextResultAsync();
        }
      });
    }

    // Factory
    // ReSharper disable HeapView.ClosureAllocation
    // ReSharper disable HeapView.DelegateAllocation
    // ReSharper disable HeapView.ObjectAllocation.Evident
    public static Batch CreateSimple(Driver driver) =>
        new Batch(async callback => {
          using var connection = await driver.CreateConnection();

          await callback(connection, null);
        });

    public static Batch CreateUsingTransaction(Transaction transaction) =>
        new Batch(async callback => {
          await callback(transaction.NpgsqlConnection, transaction.NpgsqlTransaction);
        });

    public static Batch CreateDedicatedTransaction(Driver driver, KDPgIsolationLevel isolationLevel) =>
        new Batch(async callback => {
          using var transaction = await driver.CreateTransaction(isolationLevel);

          await callback(transaction.NpgsqlConnection, transaction.NpgsqlTransaction);
          await transaction.CommitAsync();
        });
    // ReSharper restore HeapView.ClosureAllocation
    // ReSharper restore HeapView.DelegateAllocation
    // ReSharper restore HeapView.ObjectAllocation.Evident
  }
}