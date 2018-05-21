using System;
using System.Threading.Tasks;
using KDLib;
using KDPgDriver.Builder;
using Npgsql;

namespace KDPgDriver
{
  public class Transaction : IDisposable
  {
    public Driver Driver { get; }

    private readonly NpgsqlTransaction _transaction;

    public Transaction(Driver driver, NpgsqlTransaction transaction)
    {
      Driver = driver;
      _transaction = transaction;
    }

    public void Dispose()
    {
      _transaction.Dispose();
    }

    public Task CommitAsync()
    {
      return _transaction.CommitAsync();
    }

    public QueryBuilder<TModel> CreateBuilder<TModel>() => Driver.CreateBuilder<TModel>();

    public Task<InsertQueryResult> QueryAsync<TOut>(InsertQuery<TOut> builder) where TOut : class, new()
      => Driver.QueryAsyncInternal(builder, _transaction);
    
    public Task<UpdateQueryResult> QueryAsync<TOut>(UpdateQuery<TOut> builder) where TOut : class, new()
      => Driver.QueryAsyncInternal(builder, _transaction);

    public Task<SelectQueryResult<TOut>> QueryAsync<TOut>(SelectQuery<TOut> builder) where TOut : class, new()
      => Driver.QueryAsyncInternal(builder, _transaction);
  }

  public class Driver
  {
    public string Dsn { get; }
    public string Schema { get; }

    public NpgsqlConnection Connection { get; private set; }

    public Driver(string dsn, string schema)
    {
      Dsn = dsn;
      Schema = schema;
    }

    public async Task ConnectAsync()
    {
      UrlUtils.ParserURI(Dsn, out var scheme, out var user, out var pass, out var host, out int port, out string path);
      path = path.TrimStart('/');

      var connString = $"Host={host};Database={path};Username={user}";

      Connection = new NpgsqlConnection(connString);
      await Connection.OpenAsync();

      string fn = @"
CREATE OR REPLACE FUNCTION f_escape_regexp(text) RETURNS text AS
$func$
SELECT regexp_replace($1, '([!$()*+.:<=>?[\\\]^{|}-])', '\\\1', 'g')
$func$
LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION f_escape_like(text) RETURNS text AS
$func$
SELECT replace(replace(replace($1
         , '\', '\\')  -- must come 1st
         , '%', '\%')
         , '_', '\_');
$func$
LANGUAGE sql IMMUTABLE;
";

      var t = Connection.CreateCommand();
      t.CommandText = fn;
      await t.ExecuteNonQueryAsync();
    }

    public Transaction CreateTransaction()
    {
      var tr = Connection.BeginTransaction();
      return new Transaction(this, tr);
    }

    public InsertQuery<TModel> CreateInsert<TModel>()
    {
      var b = new InsertQuery<TModel>();
      return b;
    }

    public QueryBuilder<TModel> CreateBuilder<TModel>()
    {
      var b = new QueryBuilder<TModel>();
      return b;
    }

    public Task<InsertQueryResult> QueryAsync<TOut>(InsertQuery<TOut> builder) where TOut : class, new()
      => QueryAsyncInternal(builder, null);

    public Task<UpdateQueryResult> QueryAsync<TOut>(UpdateQuery<TOut> builder) where TOut : class, new()
      => QueryAsyncInternal(builder, null);

    public Task<SelectQueryResult<TOut>> QueryAsync<TOut>(SelectQuery<TOut> builder) where TOut : class, new()
      => QueryAsyncInternal(builder, null);

    internal async Task<InsertQueryResult> QueryAsyncInternal<TOut>(InsertQuery<TOut> builder, NpgsqlTransaction trans) where TOut : class, new()
    {
      string query = builder.GetQuery(this);

      Console.WriteLine(query);

      using (var cmd = new NpgsqlCommand(query, Connection, trans)) {
        builder.Parameters.AssignToCommand(cmd);
        await cmd.ExecuteNonQueryAsync();
      }

      return null;
    }

    internal async Task<UpdateQueryResult> QueryAsyncInternal<TOut>(UpdateQuery<TOut> builder, NpgsqlTransaction trans) where TOut : class, new()
    {
      string query = builder.GetQuery(this);

      Console.WriteLine(query);

      using (var cmd = new NpgsqlCommand(query, Connection, trans)) {
        builder.Parameters.AssignToCommand(cmd);

        await cmd.ExecuteNonQueryAsync();
      }

      return null;
    }

    internal async Task<SelectQueryResult<TOut>> QueryAsyncInternal<TOut>(SelectQuery<TOut> builder, NpgsqlTransaction trans) where TOut : class, new()
    {
      var columns = builder.GetColumns();
      string query = builder.GetQuery(this);

      Console.WriteLine(query);

      var cmd = new NpgsqlCommand(query, Connection, trans);
      builder.Parameters.AssignToCommand(cmd);
      var reader = await cmd.ExecuteReaderAsync();

      return new SelectQueryResult<TOut>(cmd, reader, columns);
    }
  }
}