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
    internal enum BatchType
    {
      Simple,
      InTransaction,
      DedicatedTransaction,
    }

    private readonly BatchType _type;
    private Driver _driver;
    private Transaction _transaction;
    private KDPgIsolationLevel _isolationLevel;

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

    internal Batch(BatchType type)
    {
      _type = type;
    }

    public static Batch CreateSimple(Driver driver)
    {
      var b = new Batch(BatchType.Simple);
      b._driver = driver;
      return b;
    }

    public static Batch CreateUsingTransaction(Transaction transaction)
    {
      var b = new Batch(BatchType.InTransaction);
      b._driver = transaction.Driver;
      b._transaction = transaction;
      return b;
    }

    public static Batch CreateDedicatedTransaction(Driver driver, KDPgIsolationLevel isolationLevel)
    {
      var b = new Batch(BatchType.DedicatedTransaction);
      b._driver = driver;
      b._isolationLevel = isolationLevel;
      return b;
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

      switch (_type) {
        case BatchType.Simple:
          using (var connection = await _driver.CreateConnection())
            await DoOperation(connection, null);
          break;
        case BatchType.InTransaction:
          await DoOperation(_transaction.NpgsqlConnection, _transaction.NpgsqlTransaction);
          break;
        case BatchType.DedicatedTransaction:
          using (var transaction = await _driver.CreateTransaction(_isolationLevel)) {
            await DoOperation(transaction.NpgsqlConnection, transaction.NpgsqlTransaction);
            await transaction.CommitAsync();
          }

          break;
        default:
          throw new ArgumentOutOfRangeException();
      }
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

    public SelectMultipleQueryFluentBuilderPrep2<TModel1, TModel2> FromMany<TModel1, TModel2>()
    {
      return new SelectMultipleQueryFluentBuilderPrep2<TModel1, TModel2>(this);
    }

    public SelectMultipleQueryFluentBuilderPrep3<TModel1, TModel2, TModel3> FromMany<TModel1, TModel2, TModel3>()
    {
      return new SelectMultipleQueryFluentBuilderPrep3<TModel1, TModel2, TModel3>(this);
    }

    public SelectMultipleQueryFluentBuilderPrep4<TModel1, TModel2, TModel3, TModel4> FromMany<TModel1, TModel2, TModel3, TModel4>()
    {
      return new SelectMultipleQueryFluentBuilderPrep4<TModel1, TModel2, TModel3, TModel4>(this);
    }

    public SelectMultipleQueryFluentBuilderPrep5<TModel1, TModel2, TModel3, TModel4, TModel5> FromMany<TModel1, TModel2, TModel3, TModel4, TModel5>()
    {
      return new SelectMultipleQueryFluentBuilderPrep5<TModel1, TModel2, TModel3, TModel4, TModel5>(this);
    }
  }
}