using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using KDPgDriver.Builders;

namespace KDPgDriver.Utils
{
  public static class NodeVisitor
  {
    internal class JsonPropertyPath
    {
      public KdPgColumnDescriptor Column { get; set; }
      public List<string> JsonPath { get; } = new List<string>();
    }

    public static KdPgColumnDescriptor EvaluateFuncExpressionToColumn<TModel>(Expression<Func<TModel, object>> exp)
    {
      return EvaluateExpressionToColumn(exp.Body);
    }

    public static KdPgColumnDescriptor EvaluateFuncExpressionToColumn<TModel, T>(Expression<Func<TModel, T>> exp)
    {
      return EvaluateExpressionToColumn(exp.Body);
    }

    public static KdPgColumnDescriptor EvaluateExpressionToColumn(Expression exp)
    {
      PropertyInfo EvaluateToPropertyInfo(Expression exp2)
      {
        switch (exp2) {
          case LambdaExpression lambda:
            return EvaluateToPropertyInfo(lambda.Body);

          case MemberExpression me:
            return (PropertyInfo) me.Member;

          case UnaryExpression un:
            switch (un.NodeType) {
              case ExpressionType.Convert:
                return (PropertyInfo) ((MemberExpression) un.Operand).Member;

              default:
                throw new Exception($"unknown operator: {un.NodeType}");
            }

          default:
            throw new Exception($"invalid node: {exp.NodeType}");
        }
      }

      return Helper.GetColumn(EvaluateToPropertyInfo(exp));
    }

    public class EvaluationOptions
    {
      public readonly Dictionary<string, RawQuery.TableNamePlaceholder> ParameterToTableAlias = new Dictionary<string, RawQuery.TableNamePlaceholder>();

      public bool ExpandBooleans { get; set; } = false;
    }

    public static TypedExpression VisitFuncExpression<TModel>(Expression<Func<TModel, object>> exp, EvaluationOptions options = null)
    {
      var names = exp.Parameters.Select(x => x.Name);
      return EvaluateToTypedExpression(exp.Body, names.ToHashSet(), options);
    }

    public static TypedExpression VisitFuncExpression<TModel, T>(Expression<Func<TModel, T>> exp, EvaluationOptions options = null)
    {
      var names = exp.Parameters.Select(x => x.Name);
      return EvaluateToTypedExpression(exp.Body, names.ToHashSet(), options);
    }

    public static TypedExpression VisitFuncExpression<TModel1, TModel2, T>(Expression<Func<TModel1, TModel2, T>> exp, EvaluationOptions options = null)
    {
      var names = exp.Parameters.Select(x => x.Name);
      return EvaluateToTypedExpression(exp.Body, names.ToHashSet(), options);
    }

    public static TypedExpression VisitFuncExpression<TModel1, TModel2, TModel3, T>(Expression<Func<TModel1, TModel2, TModel3, T>> exp, EvaluationOptions options = null)
    {
      var names = exp.Parameters.Select(x => x.Name);
      return EvaluateToTypedExpression(exp.Body, names.ToHashSet(), options);
    }

    public static TypedExpression VisitFuncExpression<TModel1, TModel2, TModel3, TModel4, T>(Expression<Func<TModel1, TModel2, TModel3, TModel4, T>> exp, EvaluationOptions options = null)
    {
      var names = exp.Parameters.Select(x => x.Name);
      return EvaluateToTypedExpression(exp.Body, names.ToHashSet(), options);
    }

    public static TypedExpression EvaluateToTypedExpression(Expression expression, string inputParameterName, EvaluationOptions options = null)
    {
      return EvaluateToTypedExpression(expression, inputParameterName == null ? null : new HashSet<string> { inputParameterName }, options);
    }

    public static TypedExpression EvaluateToTypedExpression(Expression expression, HashSet<string> inputParametersNames = null, EvaluationOptions options = null)
    {
      if (options == null)
        options = new EvaluationOptions();

      TypedExpression VisitInternal(Expression exp)
      {
        TypedExpression val;

        switch (exp) {
          case MemberExpression me:
            var pi = VisitPath(options, me);
            if (options.ExpandBooleans && me.Type == typeof(bool)) // for cases like (x => x.BoolValue)
              return ExpressionBuilders.Eq(pi.Expression, TypedExpression.FromValue(true));

            return pi.Expression;

          case ConstantExpression me:
          {
            var pgValue = Helper.ConvertObjectToPgValue(me.Value);
            return new TypedExpression(RawQuery.Create(pgValue), pgValue.Type);
          }

          case UnaryExpression un:
            val = VisitInternal(un.Operand);

            switch (un.NodeType) {
              case ExpressionType.Convert:
                if (options.ExpandBooleans && un.Operand is MemberExpression && un.Operand.Type == typeof(bool)) // for cases like (x => x.BoolValue)
                  return ExpressionBuilders.Eq(val, TypedExpression.FromValue(true));
                return val;
              case ExpressionType.Not: return ExpressionBuilders.Not(val);
              default:
                throw new Exception($"unknown operator: {un.NodeType}");
            }

          case BinaryExpression be:
            TypedExpression left = VisitInternal(be.Left);
            TypedExpression right = VisitInternal(be.Right);

            switch (be.NodeType) {
              case ExpressionType.Equal: return ExpressionBuilders.Eq(left, right);
              case ExpressionType.NotEqual: return ExpressionBuilders.NotEq(left, right);
              case ExpressionType.Add: return ExpressionBuilders.Add(left, right);
              case ExpressionType.Subtract: return ExpressionBuilders.Subtract(left, right);
              case ExpressionType.Multiply: return ExpressionBuilders.Multiply(left, right);
              case ExpressionType.Divide: return ExpressionBuilders.Divide(left, right);
              case ExpressionType.AndAlso: return ExpressionBuilders.And(new[] { left, right });
              case ExpressionType.OrElse: return ExpressionBuilders.Or(new[] { left, right });
              case ExpressionType.LessThan: return ExpressionBuilders.LessThan(left, right);
              case ExpressionType.LessThanOrEqual: return ExpressionBuilders.LessThanEqual(left, right);
              case ExpressionType.GreaterThan: return ExpressionBuilders.GreaterThan(left, right);
              case ExpressionType.GreaterThanOrEqual: return ExpressionBuilders.GreaterThanEqual(left, right);
              default: throw new Exception($"unknown operator: {be.NodeType}");
            }

          case MethodCallExpression call:
            // Native methods
            if (call.Method.Name == "Substring") {
              TypedExpression callObject = VisitInternal(call.Object);
              TypedExpression start = VisitInternal(call.Arguments[0]);
              TypedExpression length = VisitInternal(call.Arguments[1]);
              return ExpressionBuilders.Substring(callObject, start, length);
            }
            else if (call.Method.Name == "StartsWith") {
              TypedExpression callObject = VisitInternal(call.Object);
              TypedExpression arg1 = VisitInternal(call.Arguments[0]);
              return ExpressionBuilders.StartsWith(callObject, arg1);
            }
            else if (call.Method.Name == "EndsWith") {
              TypedExpression callObject = VisitInternal(call.Object);
              TypedExpression arg1 = VisitInternal(call.Arguments[0]);
              return ExpressionBuilders.EndsWith(callObject, arg1);
            }
            else if (call.Method.Name == "Contains") {
              TypedExpression callObject = VisitInternal(call.Object);
              TypedExpression arg1 = VisitInternal(call.Arguments[0]);
              return ExpressionBuilders.Contains(callObject, arg1);
            }
            else if (call.Method.Name == "ToLower") {
              TypedExpression callObject = VisitInternal(call.Object);
              return ExpressionBuilders.ToLower(callObject);
            }
            else if (call.Method.Name == "ToUpper") {
              TypedExpression callObject = VisitInternal(call.Object);
              return ExpressionBuilders.ToUpper(callObject);
            }
            // Native accessors
            else if (call.Method.Name == "get_Item") {
              TypedExpression callObject = VisitInternal(call.Object);
              TypedExpression arg1 = VisitInternal(call.Arguments[0]);

              RawQuery rq = new RawQuery();
              rq.AppendSurround(callObject.RawQuery);
              rq.Append("->");
              rq.Append(arg1.RawQuery);

              return new TypedExpression(rq, KDPgValueTypeInstances.Json);
            }
            // Extension methods
            else if (call.Method.Name == "PgIn") {
              TypedExpression extensionObject = VisitInternal(call.Arguments[0]);
              var arg1 = GetConstant(call.Arguments[1]);
              return ExpressionBuilders.In(extensionObject, (IEnumerable) arg1);
            }
            else if (call.Method.Name == "PgNotIn") {
              TypedExpression extensionObject = VisitInternal(call.Arguments[0]);
              var arg1 = GetConstant(call.Arguments[1]);
              return ExpressionBuilders.NotIn(extensionObject, (IEnumerable) arg1);
            }
            else if (call.Method.Name == "PgLike") {
              TypedExpression extensionObject = VisitInternal(call.Arguments[0]);
              TypedExpression arg1 = VisitInternal(call.Arguments[1]);
              return ExpressionBuilders.Like(extensionObject, arg1);
            }
            else if (call.Method.Name == "PgILike") {
              TypedExpression extensionObject = VisitInternal(call.Arguments[0]);
              TypedExpression arg1 = VisitInternal(call.Arguments[1]);
              return ExpressionBuilders.ILike(extensionObject, arg1);
            }
            else if (call.Method.Name == "PgRawLike") {
              TypedExpression extensionObject = VisitInternal(call.Arguments[0]);
              TypedExpression arg1 = VisitInternal(call.Arguments[1]);
              return ExpressionBuilders.RawLike(extensionObject, arg1);
            }
            else if (call.Method.Name == "PgRawILike") {
              TypedExpression extensionObject = VisitInternal(call.Arguments[0]);
              TypedExpression arg1 = VisitInternal(call.Arguments[1]);
              return ExpressionBuilders.RawILike(extensionObject, arg1);
            }
            else if (call.Method.Name == "PgContainsAny") {
              TypedExpression extensionObject = VisitInternal(call.Arguments[0]);
              TypedExpression arg1 = VisitInternal(call.Arguments[1]);
              return ExpressionBuilders.ContainsAny(extensionObject, arg1);
            }
            // PG funcs
            else {
              string methodName = call.Method.Name;

              // TODO: optimize
              var methods = typeof(FuncInternal).GetMethods();

              if (methodName == "Raw") {
                var valueType = Helper.CreatePgValueTypeFromObjectType(call.Method.ReturnType);
                var text = (string) ((ConstantExpression) call.Arguments[0]).Value;
                return new TypedExpression(RawQuery.Create(text), valueType);
              }
              else {
                var internalMethod = methods.SingleOrDefault(x => x.Name == methodName);
                if (internalMethod == null)
                  throw new Exception($"invalid method: {call.Method.Name}");

                var passedArgsCount = call.Arguments.Count;
                var methodArgsCount = internalMethod.GetParameters().Length;

                var args = call.Arguments
                               .Select(VisitInternal)
                               .Concat(Enumerable.Repeat(Type.Missing, Math.Max(0, methodArgsCount - passedArgsCount)))
                               .ToArray();

                return (TypedExpression) internalMethod.Invoke(null, args);
              }
            }

          default:
            throw new Exception($"invalid node: {(exp == null ? "(null)" : exp.NodeType.ToString())}");
        }
      }

      return VisitInternal(Evaluator.PartialEval(expression, inputParametersNames));
    }

    public class PathInfo
    {
      public TypedExpression Expression;
      public KdPgColumnDescriptor Column { get; set; }
      public List<object> JsonPath { get; } = new List<object>();
      public string ParameterName;
    }

    public static PathInfo VisitPath<TModel>(EvaluationOptions options, Expression<Func<TModel, object>> exp)
    {
      return VisitPath(options, exp.Body);
    }

    public static PathInfo VisitPath(EvaluationOptions options, Expression exp)
    {
      var pi = new PathInfo();
      var rq = new RawQuery();
      KDPgValueType pathValueType = null;

      List<object> parts = new List<object>();

      // extract Body expression if Func expression was passed
      if (exp is LambdaExpression lm)
        exp = lm.Body;

      // remove Convert from last part
      if (exp is UnaryExpression un && un.NodeType == ExpressionType.Convert) {
        exp = (MemberExpression) un.Operand;
      }

      void Traverse(Expression innerExpression)
      {
        Expression parentExpression;
        switch (innerExpression) {
          case MemberExpression memberExpression:
            parentExpression = memberExpression.Expression;
            var member = (PropertyInfo) memberExpression.Member;

            Traverse(parentExpression);

            parts.Add(member);

            break;

          case MethodCallExpression callExpression:
            Debug.Assert(callExpression.Method.Name == "get_Item");
            parentExpression = callExpression.Object;
            var indexValue = GetConstant(callExpression.Arguments.First());

            Traverse(parentExpression);

            parts.Add(indexValue);

            break;

          case ParameterExpression parameterExpression: // lambda parameter
            pi.ParameterName = parameterExpression.Name;
            break;

          default:
            throw new Exception("invalid node");
        }
      }

      Traverse(exp);

      string overrideTableName = null;
      foreach (var part in parts) {
        switch (part) {
          case PropertyInfo member:

            // table
            if (Helper.IsTable(member.PropertyType)) {
              if (parts.Count == 1) { // only process table if it is only part in the path (x => x.M1.Name)
                var table = Helper.GetTable(member.PropertyType);

                rq.AppendTable(new RawQuery.TableNamePlaceholder(table, member.Name));

                pathValueType = null;
              }
              else {
                overrideTableName = member.Name;
              }
            }
            // column
            else if (Helper.IsColumn(member)) {
              var column = Helper.GetColumn(member);
              pi.Column = column;

              if (options == null || options.ParameterToTableAlias.Count == 0) {
                var tableVarName = overrideTableName ?? column.Table.Name;
                rq.AppendColumn(column, new RawQuery.TableNamePlaceholder(column.Table, tableVarName));
              }
              else {
                var tableVarName = overrideTableName ?? pi.ParameterName;
                rq.AppendColumn(column, options.ParameterToTableAlias[tableVarName]);
              }

              pathValueType = column.Type;
            }
            // json path
            else {
              var fieldName = Helper.GetJsonPropertyName(member);
              var fieldType = Helper.GetJsonPropertyType(member);

              rq.Append("->");
              rq.Append(Helper.EscapePostgresValue(fieldName));
              pi.JsonPath.Add(fieldName);

              pathValueType = fieldType;
            }

            break;

          case string jsonObjectProperty:
            rq.Append("->");
            rq.Append(Helper.EscapePostgresValue(jsonObjectProperty));
            pi.JsonPath.Add(jsonObjectProperty);

            pathValueType = KDPgValueTypeInstances.Json;
            break;

          case int jsonArrayIndex:
            rq.Append("->");
            rq.Append(jsonArrayIndex.ToString());
            pi.JsonPath.Add(jsonArrayIndex);

            pathValueType = KDPgValueTypeInstances.Json;
            break;

          default:
            throw new Exception("invalid node");
        }
      }

      if (pi.JsonPath.Count > 0) {
        // cast to native type if known
        if (pathValueType != KDPgValueTypeInstances.Json) {
          RawQuery rq2 = new RawQuery();
          rq2.AppendSurround(rq);

          if (pathValueType == KDPgValueTypeInstances.String)
            rq2.Append("::", pathValueType.PostgresTypeName);
          else
            rq2.Append("::text::", pathValueType.PostgresTypeName);

          rq = rq2;
        }
      }


      pi.Expression = new TypedExpression(rq, pathValueType);

      return pi;
    }

    private static object GetConstant(Expression e)
    {
      switch (e) {
        case ConstantExpression me:
          return me.Value;
        case UnaryExpression un:
          switch (un.NodeType) {
            case ExpressionType.Convert:
              return ((ConstantExpression) un.Operand).Value;

            default:
              throw new Exception($"unknown operator: {un.NodeType}");
          }
        default:
          throw new Exception($"invalid node: {(e == null ? "(null)" : e.NodeType.ToString())}");
      }
    }
  }
}