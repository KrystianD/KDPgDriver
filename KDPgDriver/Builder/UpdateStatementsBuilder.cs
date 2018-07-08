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
          UpdateQuery.updateParts.Add(colName, RawQuery.Create(npgValue));
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

      // var colDesc = Helper.GetColumn(field.Body as MemberExpression);

      var valueArray = new[] { value };
      var pgValue = Helper.ConvertObjectToPgValue(valueArray);
      // var valueType = Helper.GetNpgsqlTypeFromObject(valueArray);
      // string val = UpdateQuery.Parameters.GetNextParam(new Helper.PgValue(valueArray, null, null));

      if (v.Type is KDPgValueTypeArray) {
        string colName = NodeVisitor.VisitProperty(field.Body);
        // var colName = v.RawQuery.RenderSimple();
        AddUpdate(colName,
                  src => RawQuery.Create("array_cat(").Append(src).Append(", ").Append(pgValue).Append(")"));
      }
      else if (v.Type is KDPgValueTypeJson) {
        string jsonPathStr1 = jsonPath.jsonPath.Select(x => $"'{x}'").JoinString(",");
        AddUpdate(jsonPath.columnName,
                  src => RawQuery.Create("kdpg_jsonb_add(").Append(src).Append(", ").Append($"array[{jsonPathStr1}], to_jsonb(").Append(pgValue).Append(")"));
      }
      else {
        throw new Exception("unable to add to non-list");
      }

      return this;
    }

    public UpdateStatementsBuilder<TModel> RemoveFromList<TValue>(Expression<Func<TModel, IList<TValue>>> field, TValue value)
    {
      string colName = NodeVisitor.VisitProperty(field.Body);
      var pgValue = Helper.ConvertObjectToPgValue(value);

      AddUpdate(colName,
                src => RawQuery.Create("array_remove(").Append(src).Append(", ").Append(pgValue).Append(")"));
      return this;
    }

    private void AddUpdate(string columnName, Func<RawQuery, RawQuery> template)
    {
      RawQuery newSrc = UpdateQuery.updateParts.GetValueOrDefault(columnName, RawQuery.CreateColumnName(columnName));
      UpdateQuery.updateParts[columnName] = template(newSrc);
    }
  }
}