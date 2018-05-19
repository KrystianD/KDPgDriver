using System;
using System.Linq.Expressions;

namespace KDPgDriver.Builder {
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
  }
}