using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data.Common;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using KDLib;
using KDPgDriver.Utils;
using Newtonsoft.Json.Linq;
using Npgsql;

namespace KDPgDriver
{
  public class SelectQueryResult<T> : IDisposable where T : class, new()
  {
    private readonly NpgsqlCommand _cmd;
    private readonly DbDataReader _reader;
    private readonly IList<PropertyInfo> _columns;

    private bool disposed = false;

    public SelectQueryResult(NpgsqlCommand cmd, DbDataReader reader, IList<PropertyInfo> columns)
    {
      _cmd = cmd;
      _reader = reader;
      _columns = columns;
    }

    public Task<bool> HasNextResult()
    {
      return _reader.ReadAsync();
    }

    public T GetCurrentResult()
    {
      var obj = new T();

      for (int i = 0; i < _reader.FieldCount; i++) {
        var rawValue = _reader.GetValue(i);

        if (_reader.IsDBNull(i))
          rawValue = null;

        var columnProperty = _columns[i];
        var propType = columnProperty.PropertyType;

        if (rawValue != null) {
          if (propType.IsGenericList()) {
            rawValue = Activator.CreateInstance(columnProperty.PropertyType, rawValue);
          }

          if (propType == typeof(JObject)) {
            rawValue = JObject.Parse((string) rawValue);
          }
        }

        columnProperty.SetValue(obj, rawValue);


        if (rawValue is Array a) {
          Console.Write("[");
          foreach (var item in a) {
            Console.Write(item);
            Console.Write(",");
          }

          Console.Write("]");
        }
        else {
          Console.Write(rawValue);
        }

        Console.Write(",");
      }

      Console.WriteLine();

      return obj;
    }

    public async Task<IList<T>> GetAll()
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
        throw new Exception("no reults found");
      var res = GetCurrentResult();
      Dispose();
      return res;
    }

    public async Task<T> GetSingleOrDefault()
    {
      bool hasResult = await HasNextResult();
      var res = hasResult ? GetCurrentResult() : null;
      Dispose();
      return res;
    }

    public void Dispose()
    {
      if (disposed)
        return;
      disposed = true;
      _reader.Dispose();
      _cmd.Dispose();
    }
  }
}