using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders
{
  public class UpdateStatementsBuilder<TModel>
  {
    internal readonly Dictionary<KdPgColumnDescriptor, TypedExpression> UpdateParts = new Dictionary<KdPgColumnDescriptor, TypedExpression>();

    public bool IsEmpty => UpdateParts.Count == 0;

    public UpdateStatementsBuilder<TModel> SetField<TValue>(Expression<Func<TModel, TValue>> field, TValue value)
    {
      var column = NodeVisitor.EvaluateExpressionToColumn(field.Body);
      var npgValue = Helper.ConvertToPgValue(column.Type, value);

      AddUpdate(column, src => TypedExpression.FromPgValue(npgValue));
      return this;
    }

    public UpdateStatementsBuilder<TModel> AddToList<TValue>(Expression<Func<TModel, IList<TValue>>> field, TValue value)
    {
      NodeVisitor.JsonPropertyPath jsonPath;
      var v = NodeVisitor.ProcessPath(null, field.Body as MemberExpression, out jsonPath);

      if (v.Type is KDPgValueTypeArray)
        AddUpdate(jsonPath.Column, src => ExpressionBuilders.ArrayAddItem(src, value));
      else if (v.Type is KDPgValueTypeJson)
        AddUpdate(jsonPath.Column, src => ExpressionBuilders.KDPgJsonbAdd(src, jsonPath.JsonPath, value));
      else
        throw new Exception("unable to add to non-list");

      return this;
    }

    public UpdateStatementsBuilder<TModel> RemoveFromList<TValue>(Expression<Func<TModel, IList<TValue>>> field, TValue value)
    {
      var column = NodeVisitor.EvaluateExpressionToColumn(field.Body);

      AddUpdate(column, src => ExpressionBuilders.ArrayRemoveItem(src, value));
      return this;
    }

    private void AddUpdate(KdPgColumnDescriptor column, Func<TypedExpression, TypedExpression> template)
    {
      if (!UpdateParts.ContainsKey(column))
        UpdateParts[column] = column.TypedExpression;

      UpdateParts[column] = template(UpdateParts[column]);
    }
  }
}