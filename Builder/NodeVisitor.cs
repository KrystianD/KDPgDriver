using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
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
          return Helper.GetColumnName(me.Member);

        default:
          throw new Exception($"invalid node: {exp.NodeType}");
      }
    }

    public static TypedValue VisitProperty2(Expression exp)
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

    public static TypedValue Visit(Expression expression, ParametersContainer parametersContainer)
    {
      TypedValue VisitInternal(Expression exp)
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
            var npgValue = Helper.GetNpgsqlTypeFromObject(me.Value);
            return new TypedValue(parametersContainer.GetNextParam(me.Value, npgValue.NpgsqlType), npgValue);

          case UnaryExpression un:
            switch (un.NodeType) {
              case ExpressionType.Convert:
                if (un.Type.IsNullable())
                  return VisitInternal(un.Operand);

                throw new Exception($"unknown type: {un.Type}");

              default:
                throw new Exception($"unknown operator: {un.NodeType}");
            }

            break;

          case BinaryExpression be:
            TypedValue left, right;

            switch (be.NodeType) {
              case ExpressionType.Equal:
                left = VisitInternal(be.Left);
                right = VisitInternal(be.Right);

                if (left.Type is KDPgColumnJsonType)
                  return new TypedValue($"{left.Expression} = to_jsonb({right.Expression}::{right.Type.PostgresType})", KDPgColumnBooleanType.Instance);
                else
                  return new TypedValue($"{left.Expression} = {right.Expression}", KDPgColumnBooleanType.Instance);

              case ExpressionType.Add:
                left = VisitInternal(be.Left);
                right = VisitInternal(be.Right);

                string op;
                if (left.Type == KDPgColumnStringType.Instance && right.Type == KDPgColumnStringType.Instance)
                  op = "||";
                else
                  op = "+";

                return new TypedValue($"{left.Expression} {op} {right.Expression}", KDPgColumnStringType.Instance);

              case ExpressionType.AndAlso:
                left = VisitInternal(be.Left);
                right = VisitInternal(be.Right);
                return new TypedValue($"({left.Expression}) AND ({right.Expression})", KDPgColumnBooleanType.Instance);

              case ExpressionType.OrElse:
                left = VisitInternal(be.Left);
                right = VisitInternal(be.Right);
                return new TypedValue($"({left.Expression}) OR ({right.Expression})", KDPgColumnBooleanType.Instance);

              default:
                throw new Exception($"unknown operator: {be.NodeType}");
            }

          case MethodCallExpression call:
            var callObject = call.Object != null ? VisitInternal(call.Object) : null;
            string txt;

            if (call.Method.Name == "PgIn") {
              callObject = VisitInternal(call.Arguments[0]);
              var value = GetConstant(call.Arguments[1]);
              var valueType = Helper.GetNpgsqlTypeFromObject(value);

              StringBuilder sb = new StringBuilder();
              if (value is Array array) {
                foreach (var item in array) {
                  sb.Append(parametersContainer.GetNextParam(item, null));
                  sb.Append(",");
                }

                sb.Remove(sb.Length - 1, 1);
              }
              else {
                throw new Exception($"invalid array: {value.GetType()}");
              }

              return new TypedValue($"({callObject.Expression}) IN ({sb})", valueType);
            }
            else if (call.Method.Name == "Substring") {
              string start = VisitInternal(call.Arguments[0]).Expression;
              string length = VisitInternal(call.Arguments[1]).Expression;
              return new TypedValue($"substring(({callObject.Expression}) from ({start}) for ({length}))", KDPgColumnStringType.Instance);
            }
            else if (call.Method.Name == "StartsWith") {
              txt = VisitInternal(call.Arguments[0]).Expression;
              return new TypedValue($"({callObject.Expression}) LIKE (f_escape_like({txt}) || '%')", KDPgColumnStringType.Instance);
            }
            else if (call.Method.Name == "get_Item") {
              txt = VisitInternal(call.Arguments[0]).Expression;

              return new TypedValue($"({callObject.Expression})->{txt}", KDPgColumnJsonType.Instance);
            }
            else if (call.Method.Name == "Contains") {
              if (callObject.Type is KDPgColumnArrayType) {
                var value = VisitInternal(call.Arguments[0]).Expression;
                return new TypedValue($"({value}) = ANY({callObject.Expression})", KDPgColumnStringType.Instance);
              }
              else {
                throw new Exception($"Contains cannot be used on non-list");
              }
            }
            else { throw new Exception($"invalid method: {call.Method.Name}"); }

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

    public static TypedValue ProcessPath(MemberExpression me, out JsonPropertyPath jsonPath)
    {
      return ProcessPath(me.Expression, me.Member, out jsonPath);
    }

    public static TypedValue ProcessPath(Expression exp, MemberInfo propertyInfo) => ProcessPath(exp, propertyInfo, out _);

    public static TypedValue ProcessPath(Expression exp, MemberInfo propertyInfo, out JsonPropertyPath jsonPath)
    {
      jsonPath = new JsonPropertyPath();
      return ProcessPathInternal(exp, propertyInfo, jsonPath);
    }

    private static TypedValue ProcessPathInternal(Expression exp, MemberInfo propertyInfo, JsonPropertyPath jsonPath)
    {
      bool isColumn = Helper.IsColumn(propertyInfo);

      if (isColumn) {
        string columnName = Helper.GetColumnName(propertyInfo);
        jsonPath.columnName = $"\"{columnName}\"";
        return new TypedValue($"\"{columnName}\"", Helper.GetColumnDataType(((PropertyInfo) propertyInfo)));
      }
      else {
        string fieldName = Helper.GetJsonPropertyName(propertyInfo);

        var fieldType = ((FieldInfo) propertyInfo).FieldType;

        if (exp is MemberExpression memberExpression) {
          TypedValue parentField = ProcessPathInternal(memberExpression.Expression, memberExpression.Member, jsonPath);
          jsonPath.jsonPath.Add(fieldName);
          return new TypedValue($"{parentField.Expression}->'{fieldName}'", KDPgColumnJsonType.Instance);
        }
        else { throw new Exception($"invalid path"); }
      }
    }
  }
}