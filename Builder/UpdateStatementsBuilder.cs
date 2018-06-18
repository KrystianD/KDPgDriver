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
    public readonly UpdateQuery<TModel> UpdateQuery;

    public UpdateStatementsBuilder(UpdateQuery<TModel> updateQuery)
    {
      UpdateQuery = updateQuery;
    }

    public UpdateStatementsBuilder<TModel> SetField<TValue>(Expression<Func<TModel, TValue>> field, TValue value)
    {
      switch (field.Body) {
        case MemberExpression memberExpression:
          PropertyInfo columnPropertyInfo = (PropertyInfo) memberExpression.Member;
          string colName = Helper.GetColumn(columnPropertyInfo).Name;
          var npgValue = Helper.ConvertToNpgsql(columnPropertyInfo, value);
          UpdateQuery.updateParts.Add(colName, UpdateQuery.Parameters.GetNextParam(npgValue));
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
      string val = UpdateQuery.Parameters.GetNextParam(new Helper.PgValue(valueArray, null, null));

      if (v.Type is KDPgValueTypeArray) {
        var colName = v.Expression;
        AddUpdate(colName, src => $"array_cat({src}, {val})");
      }
      else if (v.Type is KDPgValueTypeJson) {
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

      var val = UpdateQuery.Parameters.GetNextParam(new Helper.PgValue(value, null, null));

      AddUpdate(colName, src => $"array_remove({src}, {val})");
      return this;
    }

    private void AddUpdate(string src, Func<string, string> template)
    {
      string newSrc = UpdateQuery.updateParts.GetValueOrDefault(src, src);
      UpdateQuery.updateParts[src] = template($"{newSrc}");
    }
  }
}