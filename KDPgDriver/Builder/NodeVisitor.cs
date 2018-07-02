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

    public static string VisitProperty<TModel>(Expression<Func<TModel, object>> exp) => VisitProperty(exp.Body);

    public static string VisitProperty(Expression exp)
    {
      switch (exp) {
        case MemberExpression me:
          return Helper.GetColumn(me.Member).Name;

        default:
          throw new Exception($"invalid node: {exp.NodeType}");
      }
    }

    public static TypedExpression VisitProperty2(Expression exp)
    {
      switch (exp) {
        case MemberExpression me:
          return ProcessPath(me.Expression, me.Member);

        default:
          throw new Exception($"invalid node: {exp.NodeType}");
      }
    }

    public static PropertyInfo GetPropertyInfo<TModel>(Expression<Func<TModel, object>> exp)
    {
      switch (exp.Body) {
        case MemberExpression me:
          return (PropertyInfo) me.Member;

        case UnaryExpression un:
          switch (un.NodeType) {
            case ExpressionType.Convert:
              // if (un.Type.IsGenericType && un.Type.GetGenericTypeDefinition() == typeof(Nullable<>)) {
              // if (un.Type.IsNullable())
              return (PropertyInfo) ((MemberExpression) un.Operand).Member;

            // throw new Exception($"unknown type: {un.Type}");

            default:
              throw new Exception($"unknown operator: {un.NodeType}");
          }

          break;

        default:
          throw new Exception($"invalid node: {exp.NodeType}");
      }
    }

    public static TypedExpression Visit(Expression expression /*, ParametersContainer parametersContainer*/)
    {
      TypedExpression VisitInternal(Expression exp)
      {
        switch (exp) {
          // case NewArrayExpression newArrayExpression:
          //   var itemType = newArrayExpression.Type.GetElementType();
          //   
          //   
          //   
          //   return null;

          case MemberExpression me:
            return ProcessPath(me.Expression, me.Member);

          case ConstantExpression me:
          {
            var npgValue = Helper.GetNpgsqlTypeFromObject(me.Type);
            var pgValue = Helper.ConvertToNpgsql(npgValue, me.Value);
            return new TypedExpression(RawQuery.Create(pgValue), npgValue);
          }

          case UnaryExpression un:
            switch (un.NodeType) {
              case ExpressionType.Convert:
                if (un.Type.IsNullable())
                  return VisitInternal(un.Operand);

                throw new Exception($"unknown type: {un.Type}");

              default:
                throw new Exception($"unknown operator: {un.NodeType}");
            }

          case BinaryExpression be:
            TypedExpression left, right;
            RawQuery rq;

            TypedExpression CreateSimpleBinaryOperator(string op)
            {
              rq = new RawQuery();

              left = VisitInternal(be.Left);
              right = VisitInternal(be.Right);

              rq.AppendSurround(left.RawQuery);
              rq.Append($" {op} ");
              rq.AppendSurround(right.RawQuery);

              return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
            }

            switch (be.NodeType) {
              case ExpressionType.Equal:
                left = VisitInternal(be.Left);
                right = VisitInternal(be.Right);

                rq = new RawQuery();
                rq.AppendSurround(left.RawQuery);
                rq.Append(" = ");
                if (left.Type is KDPgValueTypeJson) {
                  rq.Append("to_jsonb(");
                  rq.AppendSurround(right.RawQuery);
                  rq.Append(")");
                }
                else {
                  rq.AppendSurround(right.RawQuery);
                }

                return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);

              // +
              case ExpressionType.Add:
                left = VisitInternal(be.Left);
                right = VisitInternal(be.Right);

                rq = new RawQuery();
                rq.AppendSurround(left.RawQuery);

                if (left.Type == KDPgValueTypeString.Instance && right.Type == KDPgValueTypeString.Instance)
                  rq.Append(" || ");
                else
                  rq.Append(" + ");

                rq.AppendSurround(right.RawQuery);

                return new TypedExpression(rq, KDPgValueTypeString.Instance);

              case ExpressionType.Subtract:
                return CreateSimpleBinaryOperator("-");
              
              case ExpressionType.Multiply:
                return CreateSimpleBinaryOperator("*");

              case ExpressionType.AndAlso:
                return CreateSimpleBinaryOperator("AND");

              case ExpressionType.OrElse:
                return CreateSimpleBinaryOperator("OR");

              case ExpressionType.GreaterThan:
                return CreateSimpleBinaryOperator(">");

              case ExpressionType.GreaterThanOrEqual:
                return CreateSimpleBinaryOperator(">=");

              case ExpressionType.LessThan:
                return CreateSimpleBinaryOperator("<");

              case ExpressionType.LessThanOrEqual:
                return CreateSimpleBinaryOperator("<=");

              default:
                throw new Exception($"unknown operator: {be.NodeType}");
            }

          case MethodCallExpression call:
            var callObject = call.Object != null ? VisitInternal(call.Object) : null;
            string callObjectStr = callObject?.RawQuery.RenderSimple();

            if (call.Method.Name == "PgIn") {
              callObject = VisitInternal(call.Arguments[0]);
              callObjectStr = callObject?.RawQuery.RenderSimple();
              var value = GetConstant(call.Arguments[1]);
              // var valueType = Helper.GetNpgsqlTypeFromObject(value);

              rq = new RawQuery();
              rq.AppendSurround(callObjectStr).Append(" IN (");

              if (value is IEnumerable array) {
                bool first = true;
                foreach (var item in array) {
                  if (!first)
                    rq.Append(",");
                  rq.AppendSurround(Helper.ConvertObjectToPgValue(item));

                  first = false;
                }
              }
              else {
                throw new Exception($"invalid array: {value.GetType()}");
              }

              rq.Append(")");

              return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
            }
            else if (call.Method.Name == "Substring") {
              string start = VisitInternal(call.Arguments[0]).RawQuery.RenderSimple();
              string length = VisitInternal(call.Arguments[1]).RawQuery.RenderSimple();
              return new TypedExpression($"substring(({callObjectStr}) from ({start}) for ({length}))", KDPgValueTypeString.Instance);
            }
            else if (call.Method.Name == "StartsWith") {
              string txt = VisitInternal(call.Arguments[0]).RawQuery.RenderSimple();
              return new TypedExpression($"({callObjectStr}) LIKE (kdpg_escape_like({txt}) || '%')", KDPgValueTypeBoolean.Instance);
            }
            else if (call.Method.Name == "get_Item") {
              string txt = VisitInternal(call.Arguments[0]).RawQuery.RenderSimple();

              return new TypedExpression($"({callObjectStr})->{txt}", KDPgValueTypeJson.Instance);
            }
            else if (call.Method.Name == "Contains") {
              if (callObject.Type is KDPgValueTypeArray) {
                rq = new RawQuery();
                rq.Append(VisitInternal(call.Arguments[0]).RawQuery);
                rq.Append(" = ANY(", callObjectStr, ")");
                return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
              }
              else {
                throw new Exception($"Contains cannot be used on non-list");
              }
            }
            else if (call.Method.Name == "PgContainsAny") {
              callObject = VisitInternal(call.Arguments[0]);
              callObjectStr = callObject?.RawQuery.RenderSimple();
              if (callObject.Type is KDPgValueTypeArray) {
                rq = new RawQuery();
                rq.AppendSurround(VisitInternal(call.Arguments[1]).RawQuery);
                rq.Append(" && ");
                rq.AppendSurround(callObjectStr);
                return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
              }
              else {
                throw new Exception($"Contains cannot be used on non-list");
              }
            }
            else {
              string methodName = call.Method.Name;

              // TODO: optimize
              var methods = typeof(Func).GetMethods();
              var method = methods.SingleOrDefault(x => x.Name == methodName);

              if (method != null) {
                var internalMethod = typeof(FuncInternal).GetMethods().Single(x => x.Name == methodName);

                var arg = VisitInternal(call.Arguments[0]);

                object[] args =
                {
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

      return VisitInternal(expression);
    }

    public class JsonPropertyPath
    {
      public string columnName;
      public List<string> jsonPath = new List<string>();
    }

    public static TypedExpression ProcessPath(MemberExpression me, out JsonPropertyPath jsonPath)
    {
      return ProcessPath(me.Expression, me.Member, out jsonPath);
    }

    public static TypedExpression ProcessPath(Expression exp, MemberInfo propertyInfo) => ProcessPath(exp, propertyInfo, out _);

    public static TypedExpression ProcessPath(Expression exp, MemberInfo propertyInfo, out JsonPropertyPath jsonPath)
    {
      jsonPath = new JsonPropertyPath();
      return ProcessPathInternal(exp, propertyInfo, jsonPath);
    }

    private static TypedExpression ProcessPathInternal(Expression exp, MemberInfo propertyInfo, JsonPropertyPath jsonPath)
    {
      bool isColumn = Helper.IsColumn(propertyInfo);

      if (isColumn) {
        var column = Helper.GetColumn(propertyInfo);
        jsonPath.columnName = $"\"{column.Name}\"";
        return new TypedExpression($"\"{column.Name}\"", column.Type);
      }
      else {
        string fieldName = Helper.GetJsonPropertyName(propertyInfo);

        // var fieldType = ((FieldInfo) propertyInfo).FieldType;

        if (exp is MemberExpression memberExpression) {
          TypedExpression parentField = ProcessPathInternal(memberExpression.Expression, memberExpression.Member, jsonPath);
          jsonPath.jsonPath.Add(fieldName);
          return new TypedExpression($"{parentField.RawQuery}->'{fieldName}'", KDPgValueTypeJson.Instance);
        }
        else { throw new Exception($"invalid path"); }
      }
    }
  }
}