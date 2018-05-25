using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using KDLib;
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
          PropertyInfo columnPropertyInfo = (PropertyInfo) memberExpression.Member;
          string colName = Helper.GetColumn(columnPropertyInfo).Name;
          var npgValue = Helper.ConvertToNpgsql(columnPropertyInfo, value);
          _updateQuery.updateParts.Add(colName, _updateQuery.Parameters.GetNextParam(npgValue.Item1, npgValue.Item2));
          break;
        default:
          throw new Exception($"invalid node: {field.Body.NodeType}");
      }

      return this;
    }

    public UpdateStatementsBuilder<TModel> AddToList<TValue>(Expression<Func<TModel, IList<TValue>>> field, TValue value)
    {
      NodeVisitor.JsonPropertyPath jsonPath;
      var v = NodeVisitor.ProcessPath(field.Body as MemberExpression, out jsonPath);

      var valueArray = new[] { value };
      var valueType = Helper.GetNpgsqlTypeFromObject(valueArray);
      string val = _updateQuery.Parameters.GetNextParam(valueArray, null);

      if (v.Type is KDPgColumnArrayType) {
        var colName = v.Expression;
        AddUpdate(colName, src => $"array_cat({src}, {val})");
      }
      else if (v.Type is KDPgColumnJsonType) {
        string jsonPathStr1 = jsonPath.jsonPath.Select(x => $"'{x}'").JoinString(",");
        AddUpdate(jsonPath.columnName,
                  src => $"kdpg_jsonb_add({src}, array[{jsonPathStr1}], to_jsonb({val}::{valueType.PostgresType}))");
      }
      else {
        throw new Exception("unable to add to non-list");
      }

      return this;
    }

    public UpdateStatementsBuilder<TModel> RemoveFromList<TValue>(Expression<Func<TModel, IList<TValue>>> field, TValue value)
    {
      string colName = NodeVisitor.VisitProperty(field.Body);

      var val = _updateQuery.Parameters.GetNextParam(value, null);
      
      AddUpdate(colName, src => $"array_remove({src}, {val})");
      return this;
    }

    private void AddUpdate(string src, Func<string, string> template)
    {
      string newSrc = _updateQuery.updateParts.GetValueOrDefault(src, src);
      _updateQuery.updateParts[src] = template($"{newSrc}");
    }
  }
}