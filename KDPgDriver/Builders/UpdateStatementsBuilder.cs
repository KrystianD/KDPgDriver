﻿using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using KDPgDriver.Traverser;
using KDPgDriver.Types;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders
{
  [Flags]
  public enum UpdateAddToListFlags
  {
    None = 0,
    Distinct = 1,
  }

  public class UpdateStatementsBuilder<TModel>
  {
    internal readonly Dictionary<KdPgColumnDescriptor, TypedExpression> UpdateParts = new Dictionary<KdPgColumnDescriptor, TypedExpression>();

    public bool IsEmpty => UpdateParts.Count == 0;

    public UpdateStatementsBuilder<TModel> UnsetField<TValue>(Expression<Func<TModel, TValue>> field)
    {
      var column = NodeVisitor.EvaluateExpressionToColumn(field.Body);

      AddUpdate(column, src => TypedExpression.FromPgValue(PgValue.Null));
      return this;
    }

    public UpdateStatementsBuilder<TModel> SetField<TValue>(Expression<Func<TModel, TValue>> field, TValue value)
    {
      var pi = NodeVisitor.VisitPath(field, null);
      var npgValue = PgTypesConverter.ConvertToPgValue(pi.Expression.Type, value);

      if (pi.JsonPath.Count > 0) {
        AddUpdate(pi.Column, src => ExpressionBuilders.JsonSet(src, pi.JsonPath, TypedExpression.FromPgValue(npgValue)));
      }
      else {
        AddUpdate(pi.Column, src => TypedExpression.FromPgValue(npgValue));
      }

      return this;
    }

    public UpdateStatementsBuilder<TModel> SetField<TValue>(Expression<Func<TModel, TValue>> field, Expression<Func<TModel, TValue>> valueExpression)
    {
      var pi = NodeVisitor.VisitPath(field, null);

      var exp = NodeVisitor.VisitFuncExpression(valueExpression);

      AddUpdate(pi.Column, src => exp);
      return this;
    }

    public UpdateStatementsBuilder<TModel> AddToList<TValue>(Expression<Func<TModel, IList<TValue>>> field, TValue value, UpdateAddToListFlags flags = UpdateAddToListFlags.None)
    {
      var pi = NodeVisitor.VisitPath(field, null);

      if (pi.Expression.Type is KDPgValueTypeArray) {
        if ((flags & UpdateAddToListFlags.Distinct) == UpdateAddToListFlags.None) {
          AddUpdate(pi.Column, src => ExpressionBuilders.ArrayAddItem(src, value));
        }
        else {
          AddUpdate(pi.Column, src => ExpressionBuilders.KDPgArrayDistinct(ExpressionBuilders.ArrayAddItem(src, value)));
        }
      }
      else if (pi.Expression.Type is KDPgValueTypeJson) {
        if (flags != 0)
          throw new Exception("flags not allowed on json array");

        AddUpdate(pi.Column, src => ExpressionBuilders.KDPgJsonbAdd(src, pi.JsonPath, value));
      }
      else
        throw new Exception("unable to add to non-list");

      return this;
    }

    public UpdateStatementsBuilder<TModel> RemoveAllFromList<TValue>(Expression<Func<TModel, IList<TValue>>> field, TValue value)
    {
      var pi = NodeVisitor.VisitPath(field, null);

      if (pi.Expression.Type is KDPgValueTypeArray)
        AddUpdate(pi.Column, src => ExpressionBuilders.ArrayRemoveItem(src, value));
      else if (pi.Expression.Type is KDPgValueTypeJson)
        AddUpdate(pi.Column, src => ExpressionBuilders.KDPgJsonbRemoveByValue(src, pi.JsonPath, value, false));
      else
        throw new Exception("unable to add to non-list");

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