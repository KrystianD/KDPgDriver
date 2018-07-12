using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using KDLib;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  public static class NodeVisitor
  {
    private static object GetConstant(Expression e)
    {
      switch (e) {
        case ConstantExpression me:
          return me.Value;
        default:
          throw new Exception($"invalid node: {(e == null ? "(null)" : e.NodeType.ToString())}");
      }
    }

    public static string VisitProperty(Expression exp)
    {
      switch (exp) {
        case MemberExpression me:
          return Helper.GetColumn(me.Member).Name;

        default:
          throw new Exception($"invalid node: {exp.NodeType}");
      }
    }

    public static KdPgColumnDescriptor EvaluateExpressionToColumn(Expression exp)
    {
      return Helper.GetColumn(NodeVisitor.EvaluateToPropertyInfo(exp));
    }

    public static PropertyInfo EvaluateToPropertyInfo(Expression exp)
    {
      switch (exp) {
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

    public static PropertyInfo EvaluateToPropertyInfo<TModel>(Expression<Func<TModel, object>> exp) => EvaluateToPropertyInfo(exp.Body);

    public static TypedExpression VisitFuncExpression<TModel>(Expression<Func<TModel, object>> exp)
    {
      return VisitToTypedExpression(exp.Body, exp.Parameters.First().Name);
    }

    public static TypedExpression VisitFuncExpression<TModel, T>(Expression<Func<TModel, T>> exp)
    {
      return VisitToTypedExpression(exp.Body, exp.Parameters.First().Name);
    }

    public static TypedExpression VisitToTypedExpression(Expression expression, string inputParameterName = null)
    {
      TypedExpression VisitInternal(Expression exp)
      {
        switch (exp) {
          // case NewArrayExpression newArrayExpression:
          //   var itemType = newArrayExpression.Type.GetElementType();
          //   return null;

          case MemberExpression me:
            return ProcessPath(me.Expression, (PropertyInfo) me.Member);

          case ConstantExpression me:
          {
            var pgValue = Helper.ConvertObjectToPgValue(me.Value);
            return new TypedExpression(RawQuery.Create(pgValue), pgValue.Type);
          }

          case UnaryExpression un:
            switch (un.NodeType) {
              case ExpressionType.Convert:
                // if (un.Type.IsNullable())
                return VisitInternal(un.Operand);

              // throw new Exception($"unknown type: {un.Type}");

              default:
                throw new Exception($"unknown operator: {un.NodeType}");
            }

          case BinaryExpression be:
            TypedExpression left = VisitInternal(be.Left);
            TypedExpression right = VisitInternal(be.Right);

            switch (be.NodeType) {
              case ExpressionType.Equal: return ExpressionBuilders.Eq(left, right);
              case ExpressionType.Add: return ExpressionBuilders.Add(left, right);
              case ExpressionType.Subtract: return ExpressionBuilders.CreateSimpleBinaryOperator(left, "-", right, false);
              case ExpressionType.Multiply: return ExpressionBuilders.CreateSimpleBinaryOperator(left, "*", right, false);
              case ExpressionType.AndAlso: return ExpressionBuilders.CreateSimpleBinaryOperator(left, "AND", right, true);
              case ExpressionType.OrElse: return ExpressionBuilders.CreateSimpleBinaryOperator(left, "OR", right, true);
              case ExpressionType.GreaterThan: return ExpressionBuilders.CreateSimpleBinaryOperator(left, ">", right, true);
              case ExpressionType.GreaterThanOrEqual: return ExpressionBuilders.CreateSimpleBinaryOperator(left, ">=", right, true);
              case ExpressionType.LessThan: return ExpressionBuilders.CreateSimpleBinaryOperator(left, "<", right, true);
              case ExpressionType.LessThanOrEqual: return ExpressionBuilders.CreateSimpleBinaryOperator(left, "<=", right, true);
              default: throw new Exception($"unknown operator: {be.NodeType}");
            }

          case MethodCallExpression call:
            var callObject = call.Object != null ? VisitInternal(call.Object) : null;

            // Native methods
            if (call.Method.Name == "Substring") {
              TypedExpression start = VisitInternal(call.Arguments[0]);
              TypedExpression length = VisitInternal(call.Arguments[1]);
              return ExpressionBuilders.Substring(callObject, start, length);
            }
            else if (call.Method.Name == "StartsWith") {
              TypedExpression value2 = VisitInternal(call.Arguments[0]);
              return ExpressionBuilders.StartsWith(callObject, value2);
            }
            else if (call.Method.Name == "Contains") {
              var arg1 = VisitInternal(call.Arguments[0]);
              return ExpressionBuilders.Contains(callObject, arg1);
            }
            // Native accessors
            else if (call.Method.Name == "get_Item") {
              var arg1 = VisitInternal(call.Arguments[0]);

              RawQuery rq = new RawQuery();
              rq.AppendSurround(callObject.RawQuery);
              rq.Append("->");
              rq.Append(arg1.RawQuery);

              return new TypedExpression(rq, KDPgValueTypeJson.Instance);
            }
            // Extension methods
            else if (call.Method.Name == "PgIn") {
              callObject = VisitInternal(call.Arguments[0]);
              var value = GetConstant(call.Arguments[1]);

              return ExpressionBuilders.In(callObject, (IEnumerable) value);
            }
            else if (call.Method.Name == "PgLike") {
              var callObject1 = VisitInternal(call.Arguments[0]);
              TypedExpression value2 = VisitInternal(call.Arguments[1]);
              return ExpressionBuilders.Like(callObject1, value2);
            }
            else if (call.Method.Name == "PgILike") {
              var callObject1 = VisitInternal(call.Arguments[0]);
              TypedExpression value2 = VisitInternal(call.Arguments[1]);
              return ExpressionBuilders.ILike(callObject1, value2);
            }
            else if (call.Method.Name == "PgContainsAny") {
              var callObject1 = VisitInternal(call.Arguments[0]);
              var arg = VisitInternal(call.Arguments[1]);
              return ExpressionBuilders.ContainsAny(callObject1, arg);
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

      return VisitInternal(Evaluator.PartialEval(expression, inputParameterName));
    }

    public class JsonPropertyPath
    {
      public string columnName;
      public List<string> jsonPath = new List<string>();
    }

    public static TypedExpression ProcessPath(MemberExpression me, out JsonPropertyPath jsonPath)
    {
      return ProcessPath(me.Expression, (PropertyInfo) me.Member, out jsonPath);
    }

    public static TypedExpression ProcessPath(Expression exp, PropertyInfo propertyInfo) => ProcessPath(exp, propertyInfo, out _);

    public static TypedExpression ProcessPath(Expression exp, PropertyInfo propertyInfo, out JsonPropertyPath jsonPath)
    {
      jsonPath = new JsonPropertyPath();
      return ProcessPathInternal(exp, propertyInfo, jsonPath);
    }

    private static TypedExpression ProcessPathInternal(Expression exp, PropertyInfo propertyInfo, JsonPropertyPath jsonPath)
    {
      bool isColumn = Helper.IsColumn(propertyInfo);

      if (isColumn) {
        var column = Helper.GetColumn(propertyInfo);
        var quotedName = Helper.QuoteObjectName(column.Name);
        jsonPath.columnName = quotedName;
        return new TypedExpression(quotedName, column.Type);
      }
      else {
        string fieldName = Helper.GetJsonPropertyName(propertyInfo);
        var fieldType = Helper.GetJsonPropertyType(propertyInfo);

        if (exp is MemberExpression memberExpression) {
          TypedExpression parentField = ProcessPathInternal(memberExpression.Expression, (PropertyInfo) memberExpression.Member, jsonPath);
          jsonPath.jsonPath.Add(fieldName);

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
    }
  }
}