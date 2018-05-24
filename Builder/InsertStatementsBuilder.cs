using System;
using System.Linq.Expressions;
using System.Reflection;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
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

          PropertyInfo columnPropertyInfo = (PropertyInfo) memberExpression.Member;
          string colName = Helper.GetColumnName(columnPropertyInfo);
          var npgValue = Helper.ConvertToNpgsql(columnPropertyInfo, value);
          _updateQuery.updateParts.Add(colName, _updateQuery.Parameters.GetNextParam(npgValue.Item1, npgValue.Item2));
          break;
        default:
          throw new Exception($"invalid node: {field.Body.NodeType}");
      }

      return this;
    }
  }
}