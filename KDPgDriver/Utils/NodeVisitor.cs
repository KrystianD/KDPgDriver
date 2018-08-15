using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
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
      public Dictionary<string, RawQuery.TableNamePlaceholder> parameterToTableAlias = new Dictionary<string, RawQuery.TableNamePlaceholder>();
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

    public static TypedExpression EvaluateToTypedExpression(Expression expression, string inputParameterName, EvaluationOptions options = null)
    {
      return EvaluateToTypedExpression(expression, inputParameterName == null ? null : new HashSet<string> { inputParameterName }, options);
    }

    public static TypedExpression EvaluateToTypedExpression(Expression expression, HashSet<string> inputParametersNames = null, EvaluationOptions options = null)
    {
      TypedExpression VisitInternal(Expression exp)
      {
        switch (exp) {
          case MemberExpression me:
            return ProcessPath(options, me.Expression, (PropertyInfo) me.Member);

          case ConstantExpression me:
          {
            var pgValue = Helper.ConvertObjectToPgValue(me.Value);
            return new TypedExpression(RawQuery.Create(pgValue), pgValue.Type);
          }

          case UnaryExpression un:
            TypedExpression val = VisitInternal(un.Operand);

            switch (un.NodeType) {
              case ExpressionType.Convert: return val;
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

              return new TypedExpression(rq, KDPgValueTypeJson.Instance);
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
              var methods = typeof(Func).GetMethods();
              var method = methods.SingleOrDefault(x => x.Name == methodName);

              if (method != null) {
                var internalMethod = typeof(FuncInternal).GetMethods().Single(x => x.Name == methodName);

                var arg = VisitInternal(call.Arguments[0]);

                object[] args = {
                    arg,
                };
                return (TypedExpression) internalMethod.Invoke(null, args);
              }
              else {
                throw new Exception($"invalid method: {call.Method.Name}");
              }
            }

          default:
            throw new Exception($"invalid node: {(exp == null ? "(null)" : exp.NodeType.ToString())}");
        }
      }

      return VisitInternal(Evaluator.PartialEval(expression, inputParametersNames));
    }

    internal static TypedExpression ProcessPath(EvaluationOptions options, MemberExpression me, out JsonPropertyPath jsonPath)
    {
      return ProcessPath(options, me.Expression, (PropertyInfo) me.Member, out jsonPath);
    }

    private static TypedExpression ProcessPath(EvaluationOptions options, Expression exp, PropertyInfo propertyInfo) => ProcessPath(options, exp, propertyInfo, out _);

    private static TypedExpression ProcessPath(EvaluationOptions options, Expression exp, PropertyInfo propertyInfo, out JsonPropertyPath jsonPath)
    {
      jsonPath = new JsonPropertyPath();
      return ProcessPathInternal(options, exp, propertyInfo, jsonPath);
    }

    private static TypedExpression ProcessPathInternal(EvaluationOptions options, Expression exp, PropertyInfo propertyInfo, JsonPropertyPath jsonPath)
    {
      bool isColumn = Helper.IsColumn(propertyInfo);
      bool isTable = Helper.IsTable(propertyInfo.PropertyType);
      string fieldName = Helper.GetJsonPropertyNameOrNull(propertyInfo);
      bool isJsonProperty = fieldName != null;


      if (isColumn) {
        var column = Helper.GetColumn(propertyInfo);
        jsonPath.Column = column;

        var tableVarName = column.Table.Name; // use table name by default (non-join mode)
        switch (exp) {
          case MemberExpression memberExpression: // member access for combined mode (join)
            tableVarName = memberExpression.Member.Name;
            break;
        }

        var rq = new RawQuery();
        if (options == null || options.parameterToTableAlias.Count == 0)
          rq.AppendColumn(column, new RawQuery.TableNamePlaceholder(column.Table, tableVarName));
        else {
          switch (exp) {
            case ParameterExpression parameterExpression: // lambda parameter
              tableVarName = parameterExpression.Name;
              break;
            case MemberExpression memberExpression: // member access for combined mode (join)
              break;
            default: throw new Exception("wrong node");
          }

          rq.AppendColumn(column, options.parameterToTableAlias[tableVarName]);
        }

        return new TypedExpression(rq, column.Type);
      }
      else if (isTable) {
        var table = Helper.GetTable(propertyInfo.PropertyType);

        var rq = new RawQuery();
        rq.AppendTable(new RawQuery.TableNamePlaceholder(table, propertyInfo.Name));
        return new TypedExpression(rq, null);
      }
      else if (isJsonProperty) {
        var fieldType = Helper.GetJsonPropertyType(propertyInfo);

        if (exp is MemberExpression memberExpression) {
          TypedExpression parentField = ProcessPathInternal(options, memberExpression.Expression, (PropertyInfo) memberExpression.Member, jsonPath);
          jsonPath.JsonPath.Add(fieldName);

          RawQuery rq = new RawQuery();
          rq.AppendSurround(parentField.RawQuery);
          rq.Append("->");
          rq.Append(Helper.EscapePostgresValue(fieldName));

          // cast to native type if known
          if (fieldType != KDPgValueTypeJson.Instance) {
            RawQuery rq2 = new RawQuery();
            rq2.AppendSurround(rq);

            if (fieldType == KDPgValueTypeString.Instance)
              rq2.Append("::", fieldType.PostgresType);
            else
              rq2.Append("::text::", fieldType.PostgresType);

            rq = rq2;
          }

          return new TypedExpression(rq, fieldType);
        }
        else { throw new Exception("invalid path"); }
      }
      else {
        throw new Exception("invalid expression");
      }
    }

    private static object GetConstant(Expression e)
    {
      switch (e) {
        case ConstantExpression me:
          return me.Value;
        default:
          throw new Exception($"invalid node: {(e == null ? "(null)" : e.NodeType.ToString())}");
      }
    }
  }
}