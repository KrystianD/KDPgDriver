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
  public class ResultColumnDef
  {
    public PropertyInfo EndModelProperty;
    public KDPgValueType Type;
  }

  public class SelectQueryResult<T>
  {
    private readonly SelectQuery<T> _builder;
    private readonly IList<ResultColumnDef> _columns;

    private List<T> objs = new List<T>();

    public SelectQueryResult(SelectQuery<T> builder, IList<ResultColumnDef> columns)
    {
      _builder = builder;
      _columns = columns;
    }

    internal async Task ProcessResultSet(NpgsqlDataReader reader)
    {
      while (await reader.ReadAsync())
        objs.Add(GetCurrentResult(reader));
    }

    private T GetCurrentResult(NpgsqlDataReader reader)
    {
      var t = typeof(T);
      T obj = default;

      object[] fields = new object[reader.FieldCount];

      for (int i = 0; i < reader.FieldCount; i++) {
        var columnProperty = _columns[i];

        object rawValue = reader.IsDBNull(i) ? null : reader.GetValue(i);

        object outputValue = Helper.ConvertFromNpgsql(columnProperty.Type, rawValue);

        fields[i] = outputValue;
      }

      if (t.IsAnonymous()) {
        obj = (T) Activator.CreateInstance(t, fields);
      }
      else {
        if (!_builder.IsSingleValue)
          obj = (T) Activator.CreateInstance(t);

        for (int i = 0; i < reader.FieldCount; i++) {
          var columnProperty = _columns[i];

          if (_builder.IsSingleValue)
            obj = (T) fields[i];
          else
            columnProperty.EndModelProperty.SetValue(obj, fields[i]);
        }
      }

      return obj;
    }

    public List<T> GetAll()
    {
      return objs;
    }

    public T GetSingle()
    {
      if (objs.Count == 0)
        throw new Exception("no results found");
      return objs[0];
    }

    public T GetSingleOrDefault()
    {
      return objs.Count == 0 ? default : objs[0];
    }
  }
}