using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using KDLib;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders
{
  public static class ExpressionBuilders
  {
    public static TypedExpression Eq(TypedExpression left, TypedExpression right)
    {
      RawQuery rq = new RawQuery();
      rq.AppendSurround(left.RawQuery);

      if (right.Type == KDPgValueTypeNull.Instance) {
        rq.Append(" IS NULL");
      }
      else {
        rq.Append(" = ");
        if (left.Type == KDPgValueTypeJson.Instance) {
          rq.Append("to_jsonb(");
          rq.AppendSurround(right.RawQuery);
          rq.Append(")");
        }
        else {
          rq.AppendSurround(right.RawQuery);
        }
      }

      return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
    }

    public static TypedExpression NotEq(TypedExpression left, TypedExpression right)
    {
      return Not(Eq(left, right));
    }

    public static TypedExpression Add(TypedExpression left, TypedExpression right)
    {
      RawQuery rq = new RawQuery();
      rq.AppendSurround(left.RawQuery);

      if (left.Type == KDPgValueTypeString.Instance && right.Type == KDPgValueTypeString.Instance)
        rq.Append(" || ");
      else if (left.Type == right.Type)
        rq.Append(" + ");
      else
        throw new Exception("unsupported operation");

      rq.AppendSurround(right.RawQuery);

      return new TypedExpression(rq, left.Type);
    }

    public static TypedExpression Multiply(TypedExpression left, TypedExpression right) => BinaryOperator("*", left, right, left.Type);
    public static TypedExpression Divide(TypedExpression left, TypedExpression right) => BinaryOperator("/", left, right, left.Type);
    public static TypedExpression Subtract(TypedExpression left, TypedExpression right) => BinaryOperator("-", left, right, left.Type);

    public static TypedExpression In(TypedExpression left, IEnumerable array)
    {
      RawQuery rq = new RawQuery();

      rq.AppendSurround(left.RawQuery)
        .Append(" = ANY(")
        .Append(Helper.ConvertObjectToPgValue(array))
        .Append(")");

      return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
    }

    public static TypedExpression NotIn(TypedExpression left, IEnumerable array)
    {
      return Not(In(left, array));
    }

    public static TypedExpression And(IEnumerable<TypedExpression> expressions) => JoinLogicExpressions("AND", expressions);
    public static TypedExpression Or(IEnumerable<TypedExpression> expressions) => JoinLogicExpressions("OR", expressions);

    public static TypedExpression Not(TypedExpression exp)
    {
      RawQuery rq = new RawQuery();
      rq.Append("NOT(");
      rq.Append(exp.RawQuery);
      rq.Append(")");

      return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
    }

    public static TypedExpression LessThan(TypedExpression left, TypedExpression right) => CreateComparisonOperator("<", left, right);
    public static TypedExpression LessThanEqual(TypedExpression left, TypedExpression right) => CreateComparisonOperator("<=", left, right);
    public static TypedExpression GreaterThan(TypedExpression left, TypedExpression right) => CreateComparisonOperator(">", left, right);
    public static TypedExpression GreaterThanEqual(TypedExpression left, TypedExpression right) => CreateComparisonOperator(">=", left, right);

    public static TypedExpression ContainsAny(TypedExpression left, IEnumerable array)
      => ContainsAny(left, TypedExpression.FromPgValue(Helper.ConvertObjectToPgValue(array)));

    public static TypedExpression ContainsAny(TypedExpression left, TypedExpression right)
    {
      RawQuery rq = new RawQuery();

      if (!(left.Type is KDPgValueTypeArray))
        throw new Exception("Contains cannot be used on non-list");

      rq.AppendSurround(left.RawQuery);
      rq.Append(" && ");
      rq.AppendSurround(right.RawQuery);

      return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
    }

    public static TypedExpression Substring(TypedExpression value, TypedExpression start, TypedExpression length)
    {
      RawQuery rq = new RawQuery();

      if (!(value.Type is KDPgValueTypeString))
        throw new Exception("Substring must be string");

      if (!(start.Type is KDPgValueTypeInteger))
        throw new Exception("start must be integer");

      if (!(length.Type is KDPgValueTypeInteger))
        throw new Exception("start must be integer");

      rq.Append("substring(");
      rq.AppendSurround(value.RawQuery);
      rq.Append(" from ");
      rq.Append(start.RawQuery);
      rq.Append(" for ");
      rq.Append(length.RawQuery);
      rq.Append(")");

      return new TypedExpression(rq, KDPgValueTypeString.Instance);
    }

    public static TypedExpression StartsWith(TypedExpression value, TypedExpression value2)
    {
      return LikeBuilder(value, value2, true, false, true, false);
    }

    public static TypedExpression EndsWith(TypedExpression value, TypedExpression value2)
    {
      return LikeBuilder(value, value2, true, true, false, false);
    }

    public static TypedExpression Like(TypedExpression value, TypedExpression value2)
    {
      return LikeBuilder(value, value2, true, true, true, false);
    }

    public static TypedExpression ILike(TypedExpression value, TypedExpression value2)
    {
      return LikeBuilder(value, value2, true, true, true, true);
    }

    public static TypedExpression RawLike(TypedExpression value, TypedExpression value2)
    {
      return LikeBuilder(value, value2, false, false, false, false);
    }

    public static TypedExpression RawILike(TypedExpression value, TypedExpression value2)
    {
      return LikeBuilder(value, value2, false, false, false, true);
    }

    private static TypedExpression LikeBuilder(TypedExpression value, TypedExpression text, bool escape, bool anyStart, bool anyEnd, bool caseInsensitive)
    {
      RawQuery rq = new RawQuery();

      if (!(value.Type is KDPgValueTypeString))
        throw new Exception("value must be string");

      if (!(text.Type is KDPgValueTypeString))
        throw new Exception("value2 must be string");

      rq.AppendSurround(value.RawQuery);
      rq.Append(caseInsensitive ? " ILIKE (" : " LIKE (");
      
      if (anyStart)
        rq.Append("'%' || ");
      
      if (escape)
        rq.Append("kdpg_escape_like(");
      rq.Append(text.RawQuery);
      if (escape)
        rq.Append(")");
      
      if (anyEnd)
        rq.Append(" || '%'");
      
      rq.Append(")");

      return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
    }

    public static TypedExpression Contains(TypedExpression value, TypedExpression value2)
    {
      if (value.Type is KDPgValueTypeArray) {
        RawQuery rq = new RawQuery();
        rq.AppendSurround(value2.RawQuery);
        rq.Append(" = ANY(");
        rq.AppendSurround(value.RawQuery);
        rq.Append(")");
        return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
      }
      else if (value.Type is KDPgValueTypeString) {
        return Like(value, value2);
      }
      else {
        throw new Exception($"Contains cannot be used on non-list");
      }
    }
    
    public static TypedExpression ToLower(TypedExpression value)
    {
      RawQuery rq = new RawQuery();
      rq.Append("lower(");
      rq.Append(value.RawQuery);
      rq.Append(")");
      return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
    }
    
    public static TypedExpression ToUpper(TypedExpression value)
    {
      RawQuery rq = new RawQuery();
      rq.Append("upper(");
      rq.Append(value.RawQuery);
      rq.Append(")");
      return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
    }

    public static TypedExpression ArrayAddItem<T>(TypedExpression array, T item)
    {
      var pgValue = Helper.ConvertObjectToPgValue(item);
      return ArrayAddItem(array, TypedExpression.FromPgValue(pgValue));
    }

    public static TypedExpression ArrayAddItem(TypedExpression array, TypedExpression item)
    {
      if (!(array.Type is KDPgValueTypeArray))
        throw new Exception("array parameter must be array");

      RawQuery rq = RawQuery.Create("array_cat(")
                            .Append(array.RawQuery)
                            .Append(", array[")
                            .Append(item.RawQuery)
                            .Append("])");

      return new TypedExpression(rq, array.Type);
    }

    public static TypedExpression ArrayRemoveItem<T>(TypedExpression array, T item)
    {
      var pgValue = Helper.ConvertObjectToPgValue(item);
      return ArrayRemoveItem(array, TypedExpression.FromPgValue(pgValue));
    }

    public static TypedExpression ArrayRemoveItem(TypedExpression array, TypedExpression item)
    {
      if (!(array.Type is KDPgValueTypeArray))
        throw new Exception("array parameter must be array");

      RawQuery rq = RawQuery.Create("array_remove(")
                            .Append(array.RawQuery)
                            .Append(", ")
                            .Append(item.RawQuery)
                            .Append(")");

      return new TypedExpression(rq, array.Type);
    }

    public static TypedExpression KDPgJsonbAdd<T>(TypedExpression array, IEnumerable<string> jsonPath, T item)
    {
      var pgValue = Helper.ConvertObjectToPgValue(item);
      return KDPgJsonbAdd(array, jsonPath, TypedExpression.FromPgValue(pgValue));
    }

    public static TypedExpression KDPgJsonbAdd(TypedExpression array, IEnumerable<string> jsonPath, TypedExpression item)
    {
      string jsonPathStr = jsonPath.Select(Helper.QuoteObjectName).JoinString(",");

      if (!(array.Type is KDPgValueTypeJson))
        throw new Exception("array parameter must be json");

      RawQuery rq = RawQuery.Create("kdpg_jsonb_add(")
                            .Append(array.RawQuery)
                            .Append(", ")
                            .Append($"array[{jsonPathStr}]")
                            .Append(", to_jsonb(")
                            .Append(item.RawQuery)
                            .Append(")");

      return new TypedExpression(rq, array.Type);
    }

    // helpers
    private static TypedExpression JoinLogicExpressions(string op, IEnumerable<TypedExpression> expressions)
    {
      RawQuery rq = new RawQuery();

      bool first = true;
      foreach (var statement in expressions) {
        if (statement.RawQuery.IsEmpty)
          continue;

        if (!first)
          rq.Append($" {op} ");

        rq.AppendSurround(statement.RawQuery);
        first = false;
      }

      return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
    }

    private static TypedExpression CreateComparisonOperator(string op, TypedExpression left, TypedExpression right)
      => BinaryOperator(op, left, right, KDPgValueTypeBoolean.Instance);

    private static TypedExpression BinaryOperator(string op, TypedExpression left, TypedExpression right, KDPgValueType outType)
    {
      RawQuery rq = new RawQuery();

      rq.AppendSurround(left.RawQuery);
      rq.Append($" {op} ");
      rq.AppendSurround(right.RawQuery);

      return new TypedExpression(rq, outType);
    }
  }
}