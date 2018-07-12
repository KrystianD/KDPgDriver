using System;
using System.Collections;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder {
  public static class ExpressionBuilders
  {
    public static TypedExpression Eq(TypedExpression left, TypedExpression right)
    {
      RawQuery rq = new RawQuery();
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

    public static TypedExpression In(TypedExpression left, IEnumerable array)
    {
      RawQuery rq = new RawQuery();

      rq.AppendSurround(left.RawQuery)
        .Append(" = ANY(")
        .Append(Helper.ConvertObjectToPgValue(array))
        .Append(")");

      return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
    }

    public static TypedExpression CreateSimpleBinaryOperator(TypedExpression left, string op, TypedExpression right, bool isBoolean)
    {
      RawQuery rq = new RawQuery();

      rq.AppendSurround(left.RawQuery);
      rq.Append($" {op} ");
      rq.AppendSurround(right.RawQuery);

      var type = isBoolean ? KDPgValueTypeBoolean.Instance : left.Type;

      return new TypedExpression(rq, type);
    }

    public static TypedExpression ContainsAny(TypedExpression left, IEnumerable array)
    {
      var pgValue = Helper.ConvertObjectToPgValue(array);
      return ContainsAny(left, TypedExpression.FromPgValue(pgValue));
    }

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
      RawQuery rq = new RawQuery();

      if (!(value.Type is KDPgValueTypeString))
        throw new Exception("value must be string");

      if (!(value2.Type is KDPgValueTypeString))
        throw new Exception("value2 must be string");

      rq.AppendSurround(value.RawQuery);
      rq.Append(" LIKE (kdpg_escape_like(");
      rq.Append(value2.RawQuery);
      rq.Append(") || '%')");

      return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
    }

    public static TypedExpression Like(TypedExpression value, TypedExpression value2)
    {
      RawQuery rq = new RawQuery();

      if (!(value.Type is KDPgValueTypeString))
        throw new Exception("value must be string");

      if (!(value2.Type is KDPgValueTypeString))
        throw new Exception("value2 must be string");

      rq.AppendSurround(value.RawQuery);
      rq.Append(" LIKE ('%' || kdpg_escape_like(");
      rq.Append(value2.RawQuery);
      rq.Append(") || '%')");

      return new TypedExpression(rq, KDPgValueTypeBoolean.Instance);
    }

    public static TypedExpression ILike(TypedExpression value, TypedExpression value2)
    {
      RawQuery rq = new RawQuery();

      if (!(value.Type is KDPgValueTypeString))
        throw new Exception("value must be string");

      if (!(value2.Type is KDPgValueTypeString))
        throw new Exception("value2 must be string");

      rq.AppendSurround(value.RawQuery);
      rq.Append(" ILIKE ('%' || kdpg_escape_like(");
      rq.Append(value2.RawQuery);
      rq.Append(") || '%')");

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
  }
}