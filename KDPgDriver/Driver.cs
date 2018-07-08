using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using KDLib;
using KDPgDriver.Builder;
using Npgsql;

namespace KDPgDriver
{
  public class Transaction : IDisposable
  {
    public Driver Driver { get; }

    private readonly NpgsqlConnection _connection;
    private readonly NpgsqlTransaction _transaction;

    public Transaction(Driver driver, NpgsqlConnection connection, NpgsqlTransaction transaction)
    {
      Driver = driver;
      _connection = connection;
      _transaction = transaction;
    }

    public void Dispose()
    {
      _connection.Close();
      _connection.Dispose();
      _transaction.Dispose();
    }

    public Task CommitAsync()
    {
      return _transaction.CommitAsync();
    }


    public Task<InsertQueryResult> QueryAsync<TOut>(InsertQuery<TOut> builder) where TOut : class
    {
      return Driver.QueryAsyncInternal(builder, _connection, _transaction);
    }

    public Task<UpdateQueryResult> QueryAsync<TOut>(UpdateQuery<TOut> builder) where TOut : class
    {
      return Driver.QueryAsyncInternal(builder, _connection, _transaction);
    }

    public Task<SelectQueryResult<TOut>> QueryAsync<TOut>(SelectQuery<TOut> builder) where TOut : class
    {
      return Driver.QueryAsyncInternal(builder, _connection, _transaction, disposeConnection: false);
    }
  }

  public class Driver
  {
    private string _connString;

    // public string Dsn { get; }
    public string Schema { get; }

    public Driver(string dsn, string schema)
    {
      // Dsn = dsn;
      Schema = schema;

      UrlUtils.ParserURI(dsn, out var scheme, out var user, out var pass, out var host, out int port, out string path);
      path = path.TrimStart('/');

      _connString = new NpgsqlConnectionStringBuilder
      {
          Database = path,
          Username = user,
          Password = pass,
          Host = host,
          Port = port,
          Pooling = true
      }.ToString();
    }

    private async Task<NpgsqlConnection> CreateConnection()
    {
      var connection = new NpgsqlConnection(_connString);
      await connection.OpenAsync();
      return connection;
    }


    // ReSharper disable once UnusedMember.Global
    public async Task InitializeAsync()
    {
      using (var connection = await CreateConnection()) {
        string fn = @"
CREATE OR REPLACE FUNCTION kdpg_escape_regexp(text) RETURNS text AS
$func$
SELECT regexp_replace($1, '([!$()*+.:<=>?[\\\]^{|}-])', '\\\1', 'g')
$func$
LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION kdpg_escape_like(text) RETURNS text AS
$func$
SELECT replace(replace(replace($1
         , '\', '\\')  -- must come 1st
         , '%', '\%')
         , '_', '\_');
$func$
LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION kdpg_jsonb_add(data jsonb, path varchar[], new_value jsonb) RETURNS jsonb AS $$
BEGIN
  RETURN jsonb_set(data, path, jsonb_extract_path(data, VARIADIC path) || new_value);
END;
$$ LANGUAGE plpgsql IMMUTABLE;
";

        using (var t = connection.CreateCommand()) {
          t.CommandText = fn;
          await t.ExecuteNonQueryAsync();
        }
      }
    }


    public async Task<Transaction> CreateTransaction()
    {
      var connection = await CreateConnection();
      var tr = connection.BeginTransaction();
      return new Transaction(this, connection, tr);
    }

    public async Task<InsertQueryResult> QueryAsync<TOut>(InsertQuery<TOut> insertQuery) where TOut : class
    {
      using (var connection = await CreateConnection()) {
        return await QueryAsyncInternal(insertQuery, connection, null);
      }
    }

    public async Task<UpdateQueryResult> QueryAsync<TOut>(UpdateQuery<TOut> updateQuery) where TOut : class
    {
      using (var connection = await CreateConnection()) {
        return await QueryAsyncInternal(updateQuery, connection, null);
      }
    }

    public async Task<SelectQueryResult<TOut>> QueryAsync<TOut>(SelectQuery<TOut> selectQuery) where TOut : class
    {
      var connection = await CreateConnection();
      return await QueryAsyncInternal(selectQuery, connection, null, disposeConnection: true);
    }

    public async Task<List<TOut>> QueryGetAllAsync<TOut>(SelectQuery<TOut> selectQuery) where TOut : class
    {
      var res = await QueryAsync(selectQuery);
      var objects = await res.GetAll();
      return objects;
    }

    // public async Task<SelectQueryResult<TOut>> QueryAsync<TModel,TOut>(Func<QueryBuilder<TModel>, SelectQuery<TOut>> fn) where TOut : class
    // {
    //   var builder = CreateBuilder<TModel>();
    //   var q = fn(builder);
    //   
    //   var connection = await CreateConnection();
    //   return await QueryAsyncInternal(q, connection, null, disposeConnection: true);
    // }

    internal async Task<InsertQueryResult> QueryAsyncInternal<TOut>(InsertQuery<TOut> builder,
                                                                    NpgsqlConnection connection,
                                                                    NpgsqlTransaction trans) where TOut : class
    {
      RawQuery rq = builder.GetQuery(this);

      string query;
      ParametersContainer parameters;
      rq.Render(out query, out parameters);
      
      Console.WriteLine(query);

      using (var cmd = new NpgsqlCommand(query, connection, trans)) {
        parameters.AssignToCommand(cmd);
        var lastInsertId = await cmd.ExecuteScalarAsync();

        return new InsertQueryResult((int) lastInsertId);
      }
    }

    internal async Task<UpdateQueryResult> QueryAsyncInternal<TOut>(UpdateQuery<TOut> builder,
                                                                    NpgsqlConnection connection,
                                                                    NpgsqlTransaction trans) where TOut : class
    {
      RawQuery rq = builder.GetQuery(this);

      string query;
      ParametersContainer parameters;
      rq.Render(out query, out parameters);

      Console.WriteLine(query);

      using (var cmd = new NpgsqlCommand(query, connection, trans)) {
        parameters.AssignToCommand(cmd);
        await cmd.ExecuteNonQueryAsync();
      }

      return null;
    }

    internal async Task<SelectQueryResult<TOut>> QueryAsyncInternal<TOut>(
        SelectQuery<TOut> builder,
        NpgsqlConnection connection,
        NpgsqlTransaction trans,
        bool disposeConnection) where TOut : class
    {
      var columns = builder.GetColumns();
      RawQuery rq = builder.GetQuery(this);

      string query;
      ParametersContainer parameters;
      rq.Render(out query, out parameters);

      Console.WriteLine(query);

      using (var cmd = new NpgsqlCommand(query, connection, trans)) {
        parameters.AssignToCommand(cmd);
        var reader = await cmd.ExecuteReaderAsync();

        return new SelectQueryResult<TOut>(connection, cmd, reader, builder, columns, disposeConnection);
      }
    }
  }
}