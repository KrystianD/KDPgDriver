using System;
using System.Collections.Generic;
using Npgsql;
using NpgsqlTypes;

namespace KDPgDriver.Builder
{
  public class ParametersContainer
  {
    private readonly List<Tuple<object, NpgsqlDbType?>> _params = new List<Tuple<object, NpgsqlDbType?>>();

    public string GetNextParam(object value, NpgsqlDbType? type)
    {
      if (value is string s) {
        if (s.Length < 30) {
          return "'" + s.Replace("'", "''") + "'";
        }
      }

      if (value == null)
        return "NULL";

      var name = $"@{_params.Count}";
      _params.Add(Tuple.Create(value, type));
      return name;
    }

    public List<Tuple<object, NpgsqlDbType?>> GetParametersList() => _params;

    public void AssignToCommand(NpgsqlCommand cmd)
    {
      for (int i = 0; i < _params.Count; i++) {
        if (_params[i].Item2.HasValue)
          cmd.Parameters.AddWithValue($"{i}", _params[i].Item2.Value, _params[i].Item1);
        else
          cmd.Parameters.AddWithValue($"{i}", _params[i].Item1);
      }
    }
  }
}