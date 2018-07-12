using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using KDLib;
using KDPgDriver.Builders;
using KDPgDriver.Fluent;
using KDPgDriver.Queries;
using KDPgDriver.Results;
using KDPgDriver.Utils;
using Npgsql;

namespace KDPgDriver
{
  public class Driver : IQueryExecutor
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

      _connString = new NpgsqlConnectionStringBuilder {
          Database = path,
          Username = user,
          Password = pass,
          Host = host,
          Port = port,
          Pooling = true
      }.ToString();
    }

    internal async Task<NpgsqlConnection> CreateConnection()
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

    public Batch CreateBatch() => new Batch(this);

    public async Task<InsertQueryResult> QueryAsync<TOut>(InsertQuery<TOut> insertQuery)
    {
      using (var connection = await CreateConnection()) {
        return await QueryAsyncInternal(insertQuery, connection, null);
      }
    }

    public async Task<UpdateQueryResult> QueryAsync<TOut>(UpdateQuery<TOut> updateQuery)
    {
      using (var connection = await CreateConnection()) {
        return await QueryAsyncInternal(updateQuery, connection, null);
      }
    }

    public async Task<SelectQueryResult<TOut>> QueryAsync<TOut>(SelectQuery<TOut> selectQuery)
    {
      using (var connection = await CreateConnection()) {
        return await QueryAsyncInternal(selectQuery, connection, null);
      }
    }

    public async Task<DeleteQueryResult> QueryAsync(DeleteQuery updateQuery)
    {
      using (var connection = await CreateConnection()) {
        return await QueryAsyncInternal(updateQuery, connection, null);
      }
    }

    internal async Task<SelectQueryResult<TOut>> QueryAsyncInternal<TOut>(SelectQuery<TOut> builder,
                                                                          NpgsqlConnection connection,
                                                                          NpgsqlTransaction trans)
    {
      var columns = builder.GetColumns();
      RawQuery rq = builder.GetRawQuery(Schema);

      string query;
      ParametersContainer parameters;
      rq.Render(out query, out parameters);

      Console.WriteLine(query);

      using (var cmd = new NpgsqlCommand(query, connection, trans)) {
        parameters.AssignToCommand(cmd);
        using (var reader = await cmd.ExecuteReaderAsync()) {
          var res = new SelectQueryResult<TOut>(builder, columns);
          await res.ProcessResultSet((NpgsqlDataReader) reader);
          return res;
        }
      }
    }

    internal async Task<InsertQueryResult> QueryAsyncInternal<TOut>(InsertQuery<TOut> builder,
                                                                    NpgsqlConnection connection,
                                                                    NpgsqlTransaction trans)
    {
      RawQuery rq = builder.GetRawQuery(Schema);

      string query;
      ParametersContainer parameters;
      rq.Render(out query, out parameters);

      Console.WriteLine(query);

      using (var cmd = new NpgsqlCommand(query, connection, trans)) {
        parameters.AssignToCommand(cmd);
        var lastInsertId = await cmd.ExecuteScalarAsync();

        return new InsertQueryResult((int?) lastInsertId);
      }
    }

    internal async Task<UpdateQueryResult> QueryAsyncInternal<TOut>(UpdateQuery<TOut> builder,
                                                                    NpgsqlConnection connection,
                                                                    NpgsqlTransaction trans)
    {
      RawQuery rq = builder.GetRawQuery(Schema);

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

    internal async Task<DeleteQueryResult> QueryAsyncInternal(DeleteQuery builder,
                                                              NpgsqlConnection connection,
                                                              NpgsqlTransaction trans)
    {
      RawQuery rq = builder.GetRawQuery(Schema);

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

    public async Task QueryRawAsync(string query)
    {
      using (var connection = await CreateConnection()) {
        using (var t = connection.CreateCommand()) {
          t.CommandText = query;
          await t.ExecuteNonQueryAsync();
        }
      }
    }

    // Helpers
    public async Task<List<TOut>> QueryGetAllAsync<TOut>(SelectQuery<TOut> selectQuery)
    {
      var res = await QueryAsync(selectQuery);
      return res.GetAll();
    }

    public async Task<TOut> QueryGetSingleAsync<TOut>(SelectQuery<TOut> selectQuery)
    {
      var res = await QueryAsync(selectQuery);
      return res.GetSingle();
    }

    // Chains
    public SelectQueryFluentBuilder1<TModel> From<TModel>()
    {
      return new SelectQueryFluentBuilder1<TModel>(this);
    }
  }
}