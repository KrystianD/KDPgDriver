using System;
using System.Linq.Expressions;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder {
  public class InsertStatementsBuilder<TModel>
  {
    private readonly UpdateQuery<TModel> _updateQuery;

    public InsertStatementsBuilder(UpdateQuery<TModel> updateQuery)
    {
      _updateQuery = updateQuery;
    }

    public InsertStatementsBuilder<TModel> SetField<TValue>(Expression<Func<TModel, TValue>> field, TValue value)
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
  }
}