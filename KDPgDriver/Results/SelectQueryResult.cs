using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data.Common;
using System.Diagnostics;
using System.Reflection;
using System.Threading.Tasks;
using KDLib;
using KDPgDriver.Queries;
using KDPgDriver.Utils;
using Npgsql;

namespace KDPgDriver.Results
{
  public class SelectQueryResult<T>
  {
    private readonly ISelectQuery _selectQuery;
    private readonly List<T> _objects = new List<T>();

    public SelectQueryResult(ISelectQuery selectQuery)
    {
      _selectQuery = selectQuery;
    }

    internal async Task ProcessResultSet(NpgsqlDataReader reader)
    {
      var proc = _selectQuery.GetResultProcessor();

      Debug.Assert(proc.FieldsCount == reader.FieldCount, "proc.FieldsCount == reader.FieldCount");

      object[] values = new object[proc.FieldsCount];

      while (await reader.ReadAsync()) {
        for (int i = 0; i < reader.FieldCount; i++)
          values[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);

        _objects.Add((T) proc.ParseResult(values));
      }
    }

    public List<T> GetAll() => _objects;

    public T GetSingle()
    {
      if (_objects.Count == 0)
        throw new Exception("no results found");
      return _objects[0];
    }

    public T GetSingleOrDefault(T def = default) => _objects.Count == 0 ? def : _objects[0];
  }
}