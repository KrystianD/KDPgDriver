using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using KDLib;
using KDPgDriver.Queries;
using KDPgDriver.Results;
using KDPgDriver.Utils;
using Npgsql;

[assembly: InternalsVisibleTo("KDPgDriverTest")]
[assembly: InternalsVisibleTo("KDPgDriver.Tests")]

namespace KDPgDriver
{
  public enum KDPgIsolationLevel
  {
    ReadCommitted,
    RepeatableRead,
    Serializable,
  }

  public class Driver : QueryExecutor
  {
    private readonly string _connString;

    public Driver(string dsn, string schema, string appName = null, int minPoolSize = 1, int maxPoolSize = 10)
    {
      UrlUtils.ParseUri(dsn, out _, out var user, out var pass, out var host, out int port, out string path);
      path = path.TrimStart('/');

      // ReSharper disable once HeapView.ObjectAllocation.Evident
      _connString = new NpgsqlConnectionStringBuilder {
          Database = path,
          Username = user,
          Password = pass,
          Host = host,
          Port = port,
          ApplicationName = appName,
          Pooling = true,
          MinPoolSize = minPoolSize,
          MaxPoolSize = maxPoolSize,
          SearchPath = EscapeUtils.QuoteObjectName(schema),
      }.ToString();
    }

    public Driver(NpgsqlConnectionStringBuilder connectionStringBuilder)
    {
      _connString = connectionStringBuilder.ToString();
    }

    internal async Task<NpgsqlConnection> CreateConnection()
    {
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      var connection = new NpgsqlConnection(_connString);
      await connection.OpenAsync();
      return connection;
    }

    // ReSharper disable once UnusedMember.Global
    public async Task InitializeAsync()
    {
      using var connection = await CreateConnection();
      
      // ReSharper disable StringLiteralTypo
      const string PgInitCode = @"
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

CREATE OR REPLACE FUNCTION kdpg_jsonb_remove_by_value(data jsonb, path varchar[], value_to_remove jsonb, only_first bool) RETURNS jsonb AS $$
DECLARE
  tmp_data jsonb;
  item jsonb;
  index int = 0;
BEGIN
  tmp_data := jsonb_extract_path(data, VARIADIC path);

	IF only_first THEN
		FOR item IN (SELECT value FROM jsonb_array_elements(tmp_data)) LOOP
    	IF item = value_to_remove THEN
				tmp_data := tmp_data - index;
        RETURN jsonb_set(data, path, tmp_data);
			END IF;
			index := index + 1;
		END LOOP;
		RETURN data;
	ELSE
		tmp_data := jsonb_agg(el.value) FROM (SELECT value FROM jsonb_array_elements(tmp_data) WHERE value != value_to_remove) el;
    RETURN jsonb_set(data, path, tmp_data);
	END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION kdpg_array_distinct(anyarray) RETURNS anyarray AS $f$
  SELECT array_agg(DISTINCT x) FROM unnest($1) t(x);
$f$ LANGUAGE SQL IMMUTABLE;
";
      // ReSharper restore StringLiteralTypo

      using var command = connection.CreateCommand();
      
      command.CommandText = PgInitCode;
      await command.ExecuteNonQueryAsync();
    }

    public async Task<Transaction> CreateTransaction(KDPgIsolationLevel isolationLevel = KDPgIsolationLevel.ReadCommitted)
    {
      var connection = await CreateConnection();
      var tr = connection.BeginTransaction(Utils.Utils.ToIsolationLevel(isolationLevel));
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new Transaction(this, connection, tr);
    }

    public Batch CreateBatch() => Batch.CreateSimple(this);

    public Batch CreateTransactionBatch(KDPgIsolationLevel isolationLevel = KDPgIsolationLevel.ReadCommitted) => Batch.CreateDedicatedTransaction(this, isolationLevel);

    public override void ScheduleQuery(IQuery query)
    {
      throw new InvalidOperationException("Schedule works only for batch query executors");
    }

    public override async Task<InsertQueryResult> QueryAsync(IInsertQuery query)
    {
      var b = Batch.CreateSimple(this);
      var res = b.QueryAsync(query);
      await b.Execute();
      return res.Result;
    }

    public override async Task<UpdateQueryResult> QueryAsync(IUpdateQuery query)
    {
      var b = Batch.CreateSimple(this);
      var res = b.QueryAsync(query);
      await b.Execute();
      return res.Result;
    }

    public override async Task<SelectQueryResult<TOut>> QueryAsync<TModel, TOut>(SelectQuery<TModel, TOut> query)
    {
      var b = Batch.CreateSimple(this);
      var res = b.QueryAsync(query);
      await b.Execute();
      return res.Result;
    }

    public override async Task<DeleteQueryResult> QueryAsync(IDeleteQuery deleteQuery)
    {
      var b = Batch.CreateSimple(this);
      var res = b.QueryAsync(deleteQuery);
      await b.Execute();
      return res.Result;
    }

    public async Task QueryRawAsync(string query)
    {
      using var connection = await CreateConnection();
      using var command = connection.CreateCommand();

      command.CommandText = query;
      await command.ExecuteNonQueryAsync();
    }
  }
}