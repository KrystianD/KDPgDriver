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

    public BaseQueryBuilder<TModel> CreateBuilder<TModel>() => Driver.CreateBuilder<TModel>();
    
    public async Task<UpdateQueryResult> QueryAsync<TOut>(UpdateQuery<TOut> builder) where TOut : class
    {
      string query = builder.GetQuery();
      var p = builder.Parameters.GetParametersList();

      Console.WriteLine(query);

      using (var cmd = new NpgsqlCommand(query, Driver.Connection, _transaction)) {
        for (int i = 0; i < p.Count; i++)
          cmd.Parameters.AddWithValue($"{i}", p[i]);

        await cmd.ExecuteNonQueryAsync();
      }

      return null;
    }

    public async Task<SelectQueryResult<TOut>> QueryAsync<TOut>(SelectQuery<TOut> builder) where TOut : class
    {
      string query = builder.GetQuery();
      var p = builder.Parameters.GetParametersList();

      Console.WriteLine(query);

      using (var cmd = new NpgsqlCommand(query, Driver.Connection, _transaction)) {
        for (int i = 0; i < p.Count; i++)
          cmd.Parameters.AddWithValue($"{i}", p[i]);

        using (var reader = await cmd.ExecuteReaderAsync()) {
          while (await reader.ReadAsync()) {
            for (int i = 0; i < reader.FieldCount; i++) {
              var t = reader.GetValue(i);

              if (t is Array a) {
                Console.Write("[");
                foreach (var item in a) {
                  Console.Write(item);
                  Console.Write(",");
                }

                Console.Write("]");
              }
              else {
                Console.Write(t);
              }

              Console.Write(",");
            }

            Console.WriteLine();
          }
        }
      }

      return new SelectQueryResult<TOut>();
      // return builder.
    }
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

    public BaseQueryBuilder<TModel> CreateBuilder<TModel>()
    {
      var b = new BaseQueryBuilder<TModel>(this);
      return b;
    }
  }
}