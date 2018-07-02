using System;
using System.Collections.Generic;
using System.Linq;
using KDLib;
using KDPgDriver.Utils;
using Npgsql;
using NpgsqlTypes;

namespace KDPgDriver.Builder
{
  public class ParametersContainer
  {
    private readonly List<Tuple<object, NpgsqlDbType?>> _params = new List<Tuple<object, NpgsqlDbType?>>();

    public string GetNextParam(Helper.PgValue pgValue)
    {
      object value = pgValue.Value;

      var idx = _params.Count + 1;

      if (value is string s) {
        if (s.Length < 30) {
          return Helper.EscapePostgresValue(s);
        }
      }

      if (value is int v) {
        return Helper.EscapePostgresValue(v);
      }

      if (value == null)
        return "NULL";

      if (value.GetType().IsGenericEumerable()) { }

      var name = $"@{idx}";
      if (pgValue.Type != null)
        name += $"::{pgValue.Type.PostgresType}";
      _params.Add(Tuple.Create(value, pgValue.Type?.NpgsqlType));
      return name;
    }

    public List<Tuple<object, NpgsqlDbType?>> GetParametersList() => _params;

    public void AssignToCommand(NpgsqlCommand cmd)
    {
      for (int i = 0; i < _params.Count; i++) {
        if (_params[i].Item2.HasValue)
          cmd.Parameters.AddWithValue($"{i + 1}", _params[i].Item2.Value, _params[i].Item1);
        else
          cmd.Parameters.AddWithValue($"{i + 1}", _params[i].Item1);
      }
    }
  }
}