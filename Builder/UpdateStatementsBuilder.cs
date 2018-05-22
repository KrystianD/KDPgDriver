using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  public class UpdateStatementsBuilder<TModel>
  {
    private readonly UpdateQuery<TModel> _updateQuery;

    public UpdateStatementsBuilder(UpdateQuery<TModel> updateQuery)
    {
      _updateQuery = updateQuery;
    }

    public UpdateStatementsBuilder<TModel> SetField<TValue>(Expression<Func<TModel, TValue>> field, TValue value)
    {
      switch (field.Body) {
        case MemberExpression memberExpression:
          string colName = Helper.GetColumnName(memberExpression.Member);

          _updateQuery.updateParts.Add(colName, _updateQuery.Parameters.GetNextParam(value));

          break;
        default:
          throw new Exception($"invalid node: {field.Body.NodeType}");
      }

      // var v = _fromBuilder.Visit(field);
      return this;
    }

    public UpdateStatementsBuilder<TModel> AddToList<TValue>(Expression<Func<TModel, IList<TValue>>> field, TValue value)
    {
      string colName = NodeVisitor.VisitProperty(field.Body);

      var val = _updateQuery.Parameters.GetNextParam(new[] { value });

      if (_updateQuery.updateParts.ContainsKey(colName)) {
        string prevVal = _updateQuery.updateParts[colName];
        _updateQuery.updateParts[colName] = $"array_cat({prevVal}, {val})";
      }
      else {
        _updateQuery.updateParts.Add(colName, $"array_cat({colName}, {val})");
      }

      return this;
    }

    public UpdateStatementsBuilder<TModel> RemoveFromList<TValue>(Expression<Func<TModel, IList<TValue>>> field, TValue value)
    {
      string colName = NodeVisitor.VisitProperty(field.Body);

      var val = _updateQuery.Parameters.GetNextParam(value);

      if (_updateQuery.updateParts.ContainsKey(colName)) {
        string prevVal = _updateQuery.updateParts[colName];
        _updateQuery.updateParts[colName] = $"array_remove({prevVal}, {val})";
      }
      else {
        _updateQuery.updateParts.Add(colName, $"array_remove({colName}, {val})");
      }

      return this;
    }
  }
}