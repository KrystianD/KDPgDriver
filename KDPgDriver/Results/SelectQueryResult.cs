using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Reflection;
using System.Threading.Tasks;
using KDLib;
using KDPgDriver.Queries;
using KDPgDriver.Utils;
using Npgsql;

namespace KDPgDriver.Results
{
  public class SelectQueryResultAsync<T> : IDisposable where T : class, new()
  {
    public void Dispose() { }
  }

  public class ResultColumnDef
  {
    public PropertyInfo EndModelProperty;
    public KDPgValueType Type;
  }

  public class SelectQueryResult<T> : IDisposable 
  {
    private readonly NpgsqlConnection _connection;

    // private readonly NpgsqlCommand _cmd;
    private readonly DbDataReader _reader;
    private readonly SelectQuery<T> _builder;
    private readonly IList<ResultColumnDef> _columns;
    private readonly bool _disposeConnection;

    private bool disposed = false;

    public SelectQueryResult(NpgsqlConnection connection, NpgsqlCommand cmd,
                             DbDataReader reader,
                             SelectQuery<T> builder, IList<ResultColumnDef> columns, bool disposeConnection)
    {
      _connection = connection;
      // _cmd = cmd;
      _reader = reader;
      _builder = builder;
      _columns = columns;
      _disposeConnection = disposeConnection;
    }

    public Task<bool> HasNextResult()
    {
      return _reader.ReadAsync();
    }

    public T GetCurrentResult()
    {
      var t = typeof(T);
      T obj = default;

      object[] fields = new object[_reader.FieldCount];

      for (int i = 0; i < _reader.FieldCount; i++) {
        var columnProperty = _columns[i];

        object rawValue = _reader.IsDBNull(i) ? null : _reader.GetValue(i);

        object outputValue = Helper.ConvertFromNpgsql(columnProperty.Type, rawValue);

        fields[i] = outputValue;
      }

      if (t.IsAnonymous()) {
        obj = (T) Activator.CreateInstance(t, fields);
      }
      else {
        if (!_builder.IsSingleValue)
          obj = (T) Activator.CreateInstance(t);

        for (int i = 0; i < _reader.FieldCount; i++) {
          var columnProperty = _columns[i];

          if (_builder.IsSingleValue)
            obj = (T) fields[i];
          else
            columnProperty.EndModelProperty.SetValue(obj, fields[i]);
        }
      }

      return obj;
    }

    public async Task<List<T>> GetAll()
    {
      List<T> objs = new List<T>();
      while (await HasNextResult())
        objs.Add(GetCurrentResult());
      Dispose();
      return objs;
    }

    public async Task<T> GetSingle()
    {
      bool hasResult = await HasNextResult();
      if (!hasResult)
        throw new Exception("no results found");
      var res = GetCurrentResult();
      Dispose();
      return res;
    }

    public async Task<T> GetSingleOrDefault()
    {
      bool hasResult = await HasNextResult();
      var res = hasResult ? GetCurrentResult() : default;
      Dispose();
      return res;
    }

    public void Dispose()
    {
      if (disposed)
        return;
      disposed = true;
      _reader.Dispose();
      // _cmd.Dispose();

      if (_disposeConnection) {
        _connection.Dispose();
      }
    }
  }
}