using System;
using System.Collections.Generic;
using System.Linq.Expressions;
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
    private delegate Task ResultProcessorHandler(NpgsqlDataReader reader);

    private readonly RawQuery _combinedRawQuery = new RawQuery();
    private readonly List<ResultProcessorHandler> _resultProcessors = new List<ResultProcessorHandler>();
    private readonly Func<Func<NpgsqlConnection, NpgsqlTransaction, Task>, Task> _connectionCreator;

    public bool IsEmpty { get; private set; } = true;

    private Batch(Func<Func<NpgsqlConnection, NpgsqlTransaction, Task>, Task> connectionCreator)
    {
      _connectionCreator = connectionCreator;
    }

    public void ScheduleQuery(IQuery query)
    {
      _combinedRawQuery.Append(query.GetRawQuery());
      _combinedRawQuery.Append(";\n");
      IsEmpty = false;
    }

    public Task<SelectQueryResult<TOut>> QueryAsync<TModel, TOut>(SelectQuery<TModel, TOut> builder)
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

    public Task<InsertQueryResult> QueryAsync(IInsertQuery builder)
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

    public Task<UpdateQueryResult> QueryAsync(IUpdateQuery builder)
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

    public Task<DeleteQueryResult> QueryAsync(IDeleteQuery builder)
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

      _combinedRawQuery.Render(out query, out parameters);

      Console.WriteLine(query);

      await _connectionCreator(async (connection, transaction) => {
        using var cmd = new NpgsqlCommand(query, connection, transaction);

        parameters.AssignToCommand(cmd);

        using var reader = await cmd.ExecuteReaderAsync();

        foreach (var operation in _resultProcessors) {
          await operation((NpgsqlDataReader)reader);
          await reader.NextResultAsync();
        }
      });
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

    // Factory
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
  }
}