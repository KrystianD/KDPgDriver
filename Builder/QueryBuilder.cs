using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using KDPgDriver.Utils;
using Npgsql;

namespace KDPgDriver.Builder
{
  public class ParametersContainer
  {
    private readonly List<object> _params = new List<object>();

    public string GetNextParam(object value)
    {
      if (value is string s) {
        if (s.Length < 30) {
          return "'" + s.Replace("'", "''") + "'";
        }
      }

      var name = $"@{_params.Count}";
      _params.Add(value);
      return name;
    }

    public List<object> GetParametersList() => _params;

    public void AssignToCommand(NpgsqlCommand cmd)
    {
      for (int i = 0; i < _params.Count; i++)
        cmd.Parameters.AddWithValue($"{i}", _params[i]);
    }
  }

  public class QueryBuilder<TModel> : IQueryBuilder
  {
    // public Driver Driver { get; }
    public string TableName { get; }

    public ParametersContainer Parameters { get; } = new ParametersContainer();
    private readonly StringBuilder _wherePart = new StringBuilder();

    public string GetWherePart() => _wherePart.ToString();

    public QueryBuilder()
    {
      // Driver = driver;
      TableName = Helper.GetTableName(typeof(TModel));
    }

    public QueryBuilder<TModel> Where(Expression<Func<TModel, bool>> exp)
    {
      var e = Evaluator.PartialEval(exp.Body);
      var whereSql = "(" + NodeVisitor.Visit(e, Parameters).Expression + ")";

      if (_wherePart.Length > 0)
        _wherePart.Append(" AND ");
      _wherePart.Append(whereSql);

      return this;
    }

    public SelectQuery<TModel> Select()
    {
      var us = new SelectQuery<TModel>(this, Parameters);
      return us;
    }

    public SelectQuery<TNewModel> Select<TNewModel>(Expression<Func<TModel, TNewModel>> pr)
    {
      var us = new SelectQuery<TNewModel>(this, Parameters);
      us.Process(pr.Body);
      return us;
    }

    public UpdateQuery<TModel> Update(Action<UpdateStatementsBuilder<TModel>> fn)
    {
      var uq = new UpdateQuery<TModel>(this, Parameters);
      var us = new UpdateStatementsBuilder<TModel>(uq);
      fn(us);
      return uq;
    }
  }
}