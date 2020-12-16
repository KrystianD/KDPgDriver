using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using System.Threading;
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
  public enum KDPgIsolationLevel
  {
    ReadCommitted,
    RepeatableRead,
    Serializable,
  }

  public class Driver : IQueryExecutor
  {
    private readonly string _connString;

    public Driver(string dsn, string schema, string appName = null, int minPoolSize = 1, int maxPoolSize = 10)
    {
      UrlUtils.ParseUri(dsn, out _, out var user, out var pass, out var host, out int port, out string path);
      path = path.TrimStart('/');

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
          SearchPath = schema,
      }.ToString();
    }

    public Driver(NpgsqlConnectionStringBuilder connectionStringBuilder)
    {
      _connString = connectionStringBuilder.ToString();
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

        using (var t = connection.CreateCommand()) {
          t.CommandText = fn;
          await t.ExecuteNonQueryAsync();
        }
      }
    }

    public async Task<Transaction> CreateTransaction(KDPgIsolationLevel isolationLevel = KDPgIsolationLevel.ReadCommitted)
    {
      var connection = await CreateConnection();
      var tr = connection.BeginTransaction(Utils.Utils.ToIsolationLevel(isolationLevel));
      return new Transaction(this, connection, tr);
    }

    public Batch CreateBatch() => Batch.CreateSimple(this);

    public Batch CreateTransactionBatch(KDPgIsolationLevel isolationLevel = KDPgIsolationLevel.ReadCommitted) => Batch.CreateDedicatedTransaction(this, isolationLevel);

    public void ScheduleQuery(IQuery query)
    {
      throw new Exception("Schedule works only for batch query");
    }

    public async Task<InsertQueryResult> QueryAsync(IInsertQuery query)
    {
      var b = Batch.CreateSimple(this);
      var res = b.QueryAsync(query);
      await b.Execute();
      return res.Result;
    }

    public async Task<UpdateQueryResult> QueryAsync(IUpdateQuery query)
    {
      var b = Batch.CreateSimple(this);
      var res = b.QueryAsync(query);
      await b.Execute();
      return res.Result;
    }

    public async Task<SelectQueryResult<TOut>> QueryAsync<TModel, TOut>(SelectQuery<TModel, TOut> query)
    {
      var b = Batch.CreateSimple(this);
      var res = b.QueryAsync(query);
      await b.Execute();
      return res.Result;
    }

    public async Task<DeleteQueryResult> QueryAsync(IDeleteQuery query)
    {
      var b = Batch.CreateSimple(this);
      var res = b.QueryAsync(query);
      await b.Execute();
      return res.Result;
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
    public async Task<List<TOut>> QueryGetAllAsync<TModel, TOut>(SelectQuery<TModel, TOut> selectQuery)
    {
      var res = await QueryAsync(selectQuery);
      return res.GetAll();
    }

    public async Task<TOut> QueryGetSingleAsync<TModel, TOut>(SelectQuery<TModel, TOut> selectQuery)
    {
      var res = await QueryAsync(selectQuery);
      return res.GetSingle();
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
  }
}