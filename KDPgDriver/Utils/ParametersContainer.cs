using System;
using System.Collections.Generic;
using Npgsql;
using NpgsqlTypes;

namespace KDPgDriver.Utils
{
  public class ParametersContainer
  {
    private readonly List<Tuple<object, NpgsqlDbType?>> _params = new List<Tuple<object, NpgsqlDbType?>>();

    public static bool TryInline(PgValue pgValue, out string inlined)
    {
      inlined = null;

      object value = pgValue.Value;

      switch (pgValue.Type) {
        case KDPgValueTypeEnum _:
          inlined = Helper.EscapePostgresValue(value);
          return true;
      }

      switch (value) {
        case string s when pgValue.Type == KDPgValueTypeInstances.String && s.Length < 30:
          inlined = Helper.EscapePostgresValue(s);
          return true;
        case int v:
          inlined = Helper.EscapePostgresValue(v);
          return true;
        case null:
          inlined = "NULL";
          return true;
        case true:
          inlined = "TRUE";
          return true;
        case false:
          inlined = "FALSE";
          return true;
      }

      return false;
    }

    public string GetNextParam(PgValue pgValue)
    {
      object value = pgValue.Value;

      var idx = _params.Count + 1;

      var name = $"@{idx}";
      if (pgValue.Type != null)
        name += $"::{pgValue.Type.PostgresTypeName}";
      _params.Add(Tuple.Create(value, pgValue.Type?.NpgsqlDbType));
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