﻿using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace KDPgDriver.Builder {
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

    public static TypedValue Visit2(Expression e222, ParametersContainer parametersContainer)
    {
      TypedValue Visit(Expression e)
      {
        switch (e) {
          // case NewArrayExpression newArrayExpression:
          //   var itemType = newArrayExpression.Type.GetElementType();
          //   
          //   
          //   
          //   return null;

          case MemberExpression me:
            return new TypedValue(Helper.GetColumnName(me.Member), Helper.GetColumnType(me.Member));

          case ConstantExpression me:
            return new TypedValue(parametersContainer.GetNextParam(me.Value), me.Type);

          case BinaryExpression be:
            TypedValue left, right;

            switch (be.NodeType) {
              case ExpressionType.Equal:
                left = Visit(be.Left);
                right = Visit(be.Right);

                return new TypedValue($"{left.Expression} = {right.Expression}", typeof(bool));

              case ExpressionType.Add:
                left = Visit(be.Left);
                right = Visit(be.Right);

                string op;
                if (left.Type == typeof(string) && right.Type == typeof(string))
                  op = "||";
                else
                  op = "+";

                return new TypedValue($"{left.Expression} {op} {right.Expression}", typeof(string));

              case ExpressionType.AndAlso:
                left = Visit(be.Left);
                right = Visit(be.Right);
                return new TypedValue($"({left.Expression}) AND ({right.Expression})", typeof(bool));

              case ExpressionType.OrElse:
                left = Visit(be.Left);
                right = Visit(be.Right);
                return new TypedValue($"({left.Expression}) OR ({right.Expression})", typeof(bool));

              default:
                throw new Exception($"unknown operator: {be.NodeType}");
            }

          case MethodCallExpression call:
            var callObject = call.Object != null ? Visit(call.Object) : null;
            string txt;

            if (call.Method.Name == "PgIn") {
              callObject = Visit(call.Arguments[0]);
              var value = GetConstant(call.Arguments[1]);

              StringBuilder sb = new StringBuilder();
              if (value is Array array) {
                foreach (var item in array) {
                  sb.Append(parametersContainer.GetNextParam(item));
                  sb.Append(",");
                }

                sb.Remove(sb.Length - 1, 1);
              }
              else {
                throw new Exception($"invalid array: {value.GetType()}");
              }

              return new TypedValue($"({callObject.Expression}) IN ({sb})", typeof(string[]));
            }
            else if (call.Method.Name == "Substring") {
              string start = Visit(call.Arguments[0]).Expression;
              string length = Visit(call.Arguments[1]).Expression;
              return new TypedValue($"substring(({callObject.Expression}) from ({start}) for ({length}))", typeof(string));
            }
            else if (call.Method.Name == "StartsWith") {
              txt = Visit(call.Arguments[0]).Expression;
              return new TypedValue($"({callObject.Expression}) LIKE (f_escape_like({txt}) || '%')", typeof(string));
            }
            else if (call.Method.Name == "get_Item") {
              txt = Visit(call.Arguments[0]).Expression;

              return new TypedValue($"({callObject.Expression})->{txt}", typeof(object));
            }
            else if (call.Method.Name == "Contains") {
              if (callObject.Type.IsGenericType && callObject.Type.GetGenericTypeDefinition() == typeof(List<>)) {
                var value = Visit(call.Arguments[0]).Expression;
                return new TypedValue($"({value}) = ANY({callObject.Expression})", typeof(string));
              }
              else {
                throw new Exception($"Contains cannot be used on non-list");
              }
            }
            else { throw new Exception($"invalid method: {call.Method.Name}"); }

          default:
            throw new Exception($"invalid node: {(e == null ? "(null)" : e.NodeType.ToString())}");
        }
      }

      return Visit(e222);
    }
  }
}