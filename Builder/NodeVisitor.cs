using System;
using System.Collections;
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

    public static TypedExpression Visit(Expression expression, ParametersContainer parametersContainer)
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
            return new TypedExpression(parametersContainer.GetNextParam(pgValue),
                                       npgValue);
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

            break;

          case BinaryExpression be:
            TypedExpression left, right;

            switch (be.NodeType) {
              case ExpressionType.Equal:
                left = VisitInternal(be.Left);
                right = VisitInternal(be.Right);

                if (left.Type is KDPgValueTypeJson)
                  return new TypedExpression($"{left.Expression} = to_jsonb({right.Expression}::{right.Type.PostgresType})", KDPgValueTypeBoolean.Instance);
                else
                  return new TypedExpression($"{left.Expression} = {right.Expression}", KDPgValueTypeBoolean.Instance);

              case ExpressionType.Add:
                left = VisitInternal(be.Left);
                right = VisitInternal(be.Right);

                string op;
                if (left.Type == KDPgValueTypeString.Instance && right.Type == KDPgValueTypeString.Instance)
                  op = "||";
                else
                  op = "+";

                return new TypedExpression($"{left.Expression} {op} {right.Expression}", KDPgValueTypeString.Instance);

              case ExpressionType.AndAlso:
                left = VisitInternal(be.Left);
                right = VisitInternal(be.Right);
                return new TypedExpression($"({left.Expression}) AND ({right.Expression})", KDPgValueTypeBoolean.Instance);

              case ExpressionType.OrElse:
                left = VisitInternal(be.Left);
                right = VisitInternal(be.Right);
                return new TypedExpression($"({left.Expression}) OR ({right.Expression})", KDPgValueTypeBoolean.Instance);

              default:
                throw new Exception($"unknown operator: {be.NodeType}");
            }

          case MethodCallExpression call:
            var callObject = call.Object != null ? VisitInternal(call.Object) : null;
            string txt;

            if (call.Method.Name == "PgIn") {
              callObject = VisitInternal(call.Arguments[0]);
              var value = GetConstant(call.Arguments[1]);
              // var valueType = Helper.GetNpgsqlTypeFromObject(value);

              StringBuilder sb = new StringBuilder();
              if (value is IEnumerable array) {
                foreach (var item in array) {
                  sb.Append(parametersContainer.GetNextParam(new Helper.PgValue(item, null, null)));
                  sb.Append(",");
                }

                sb.Remove(sb.Length - 1, 1);
              }
              else {
                throw new Exception($"invalid array: {value.GetType()}");
              }

              return new TypedExpression($"({callObject.Expression}) IN ({sb})", KDPgValueTypeBoolean.Instance);
            }
            else if (call.Method.Name == "Substring") {
              string start = VisitInternal(call.Arguments[0]).Expression;
              string length = VisitInternal(call.Arguments[1]).Expression;
              return new TypedExpression($"substring(({callObject.Expression}) from ({start}) for ({length}))", KDPgValueTypeString.Instance);
            }
            else if (call.Method.Name == "StartsWith") {
              txt = VisitInternal(call.Arguments[0]).Expression;
              return new TypedExpression($"({callObject.Expression}) LIKE (kdpg_escape_like({txt}) || '%')", KDPgValueTypeBoolean.Instance);
            }
            else if (call.Method.Name == "get_Item") {
              txt = VisitInternal(call.Arguments[0]).Expression;

              return new TypedExpression($"({callObject.Expression})->{txt}", KDPgValueTypeJson.Instance);
            }
            else if (call.Method.Name == "Contains") {
              if (callObject.Type is KDPgValueTypeArray) {
                var value = VisitInternal(call.Arguments[0]).Expression;
                return new TypedExpression($"({value}) = ANY({callObject.Expression})", KDPgValueTypeBoolean.Instance);
              }
              else {
                throw new Exception($"Contains cannot be used on non-list");
              }
            }
            else if (call.Method.Name == "PgContainsAny") {
              callObject = VisitInternal(call.Arguments[0]);
              if (callObject.Type is KDPgValueTypeArray) {
                var value = VisitInternal(call.Arguments[1]).Expression;
                return new TypedExpression($"({value}) && {callObject.Expression}", KDPgValueTypeBoolean.Instance);
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
          return new TypedExpression($"{parentField.Expression}->'{fieldName}'", KDPgValueTypeJson.Instance);
        }
        else { throw new Exception($"invalid path"); }
      }
    }
  }
}