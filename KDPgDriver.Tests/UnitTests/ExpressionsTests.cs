using System;
using System.Collections.Generic;
using System.Linq;
using KDPgDriver.Builders;
using KDPgDriver.Traverser;
using KDPgDriver.Utils;
using Newtonsoft.Json.Linq;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests.UnitTests
{
  public class ExpressionsTests
  {
    static ExpressionsTests()
    {
      MyInit.Init();
    }

    // Logic and binary operators
    [Fact]
    public void ExpressionEq()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id == 2);

      Utils.AssertExpression(exp, @"(""id"") = (2)");
    }

    [Fact]
    public void ExpressionEqNull()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name == null);

      Utils.AssertExpression(exp, @"(""name"") IS NULL");
    }

    [Fact]
    public void ExpressionNotEq()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id != 2);

      Utils.AssertExpression(exp, @"NOT((""id"") = (2))");
    }

    [Fact]
    public void ExpressionNotEqNull()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name != null);

      Utils.AssertExpression(exp, @"NOT((""name"") IS NULL)");
    }

    [Fact]
    public void ExpressionEqJsonModelNull()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.JsonModel == null);

      Utils.AssertExpression(exp, @"(json_model) IS NULL");
    }

    [Fact]
    public void ExpressionEqJsonObjectNull()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.JsonObject1 == null);

      Utils.AssertExpression(exp, @"(json_object1) IS NULL");
    }

    [Fact]
    public void ExpressionEqLongString()
    {
      string s1 = "long string long string long string long string long string long string long string";

      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name == s1);

      Utils.AssertExpression(exp, @"(""name"") = (@1::text)",
                             new Param(s1, NpgsqlDbType.Text));
    }

    #region Operator Add

    [Fact]
    public void ExpressionAddNumbers()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id + x.Id == 1);

      Utils.AssertExpression(exp, @"((""id"") + (""id"")) = (1)");
    }

    [Fact]
    public void ExpressionAddStrings()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name + x.Name == "X");

      Utils.AssertExpression(exp, @"((""name"") || (""name"")) = ('X')");
    }

    #endregion

    [Fact]
    public void ExpressionMultiply()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id * x.Id == 1);

      Utils.AssertExpression(exp, @"((""id"") * (""id"")) = (1)");
    }

    [Fact]
    public void ExpressionSubtract()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id - x.Id == 1);

      Utils.AssertExpression(exp, @"((""id"") - (""id"")) = (1)");
    }

    [Fact]
    public void ExpressionDivide()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id / x.Id == 1);

      Utils.AssertExpression(exp, @"((""id"") / (""id"")) = (1)");
    }

    [Fact]
    public void ExpressionGreaterThan()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id > 1);

      Utils.AssertExpression(exp, @"(""id"") > (1)");
    }

    [Fact]
    public void ExpressionGreaterThanEquals()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id >= 1);

      Utils.AssertExpression(exp, @"(""id"") >= (1)");
    }

    [Fact]
    public void ExpressionLessThan()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id < 1);

      Utils.AssertExpression(exp, @"(""id"") < (1)");
    }

    [Fact]
    public void ExpressionLessThanEquals()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id <= 1);

      Utils.AssertExpression(exp, @"(""id"") <= (1)");
    }

    [Fact]
    public void ExpressionOr()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id == 1 || x.Id == 2);

      Utils.AssertExpression(exp, @"((""id"") = (1)) OR ((""id"") = (2))");
    }

    [Fact]
    public void ExpressionAnd()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id == 1 && x.Id == 2);

      Utils.AssertExpression(exp, @"((""id"") = (1)) AND ((""id"") = (2))");
    }

    // Functions
    [Fact]
    public void ExpressionSubstring()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.Substring(5 + x.Id, 10) == "A");

      Utils.AssertExpression(exp, @"(substring((""name"") from (5) + (""id"") for 10)) = ('A')");
    }

    #region Operator In

    [Fact]
    public void ExpressionInDirect()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.PgIn("A", "B"));

      Utils.AssertExpression(exp, @"(""name"") = ANY(@1::text[])",
                             new Param(new[] { "A", "B" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    [Fact]
    public void ExpressionInArray()
    {
      var items = new[] { null, "A1", "A2", "B3", "A4" };

      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.PgIn(items));

      Utils.AssertExpression(exp, @"(""name"") = ANY(@1::text[])",
                             new Param(new[] { null, "A1", "A2", "B3", "A4" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    [Fact]
    public void ExpressionInList()
    {
      var items = new List<string>() { null, "A1", "A2", "B3", "A4" };

      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.PgIn(items));

      Utils.AssertExpression(exp, @"(""name"") = ANY(@1::text[])",
                             new Param(new[] { null, "A1", "A2", "B3", "A4" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    [Fact]
    public void ExpressionInEnumerable()
    {
      var items = new List<string>() { null, "A1", "A2", "B3", "A4" };
      var items2 = items.Where(x => x == null || x.StartsWith("A")).Distinct();

      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.PgIn(items2));

      Utils.AssertExpression(exp, @"(""name"") = ANY(@1::text[])",
                             new Param(new[] { null, "A1", "A2", "A4" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    [Fact]
    public void ExpressionInEnumerableSelect()
    {
      var items = new List<string>() { null, "A1", "A2", "B3", "A4" };
      var items2 = items.Where(x => x != null && x.StartsWith("A")).Select(x => x.Length);

      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id.PgIn(items2));

      Utils.AssertExpression(exp, @"(""id"") = ANY(@1::int[])",
                             new Param(new[] { 2, 2, 2 }, NpgsqlDbType.Array | NpgsqlDbType.Integer));
    }

    [Fact]
    public void ExpressionNotInDirect()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.PgNotIn("A", "B"));

      Utils.AssertExpression(exp, @"NOT((""name"") = ANY(@1::text[]))",
                             new Param(new[] { "A", "B" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    #endregion

    [Fact]
    public void ExpressionContainsAny()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.ListString.PgContainsAny("A", "B"));

      Utils.AssertExpression(exp, @"(list_string) && (@1::text[])",
                             new Param(new[] { "A", "B" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    [Fact]
    public void ExpressionStringContains()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.Contains("A"));

      Utils.AssertExpression(exp, @"(""name"") LIKE ('%' || kdpg_escape_like('A') || '%')");
    }

    [Fact]
    public void ExpressionArrayContains()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.ListString.Contains("A"));

      Utils.AssertExpression(exp, @"('A') = ANY((list_string))");
    }

    [Fact]
    public void ExpressionStringLike()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.PgLike("A"));

      Utils.AssertExpression(exp, @"(""name"") LIKE ('%' || kdpg_escape_like('A') || '%')");
    }

    [Fact]
    public void ExpressionStringILike()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.PgILike("A"));

      Utils.AssertExpression(exp, @"(""name"") ILIKE ('%' || kdpg_escape_like('A') || '%')");
    }

    [Fact]
    public void ExpressionStringRawLike()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.PgRawLike("A"));

      Utils.AssertExpression(exp, @"(""name"") LIKE ('A')");
    }

    [Fact]
    public void ExpressionStringRawILike()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.PgRawILike("A"));

      Utils.AssertExpression(exp, @"(""name"") ILIKE ('A')");
    }

    [Fact]
    public void ExpressionStartsWith()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.StartsWith("A"));

      Utils.AssertExpression(exp, @"(""name"") LIKE (kdpg_escape_like('A') || '%')");
    }

    [Fact]
    public void ExpressionEndsWith()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.EndsWith("A"));

      Utils.AssertExpression(exp, @"(""name"") LIKE ('%' || kdpg_escape_like('A'))");
    }

    [Fact]
    public void ExpressionToLower()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.ToLower());

      Utils.AssertExpression(exp, @"lower(""name"")");
    }

    [Fact]
    public void ExpressionToUpper()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.ToUpper());

      Utils.AssertExpression(exp, @"upper(""name"")");
    }

    // Coalesce
    [Fact]
    public void ExpressionCoalesce()
    {
      var exp1 = NodeVisitor.VisitFuncExpression<MyModel>(x => Func.Coalesce(x.Id, 2));
      Utils.AssertExpression(exp1, @"COALESCE(""id"", 2)");

      var exp2 = NodeVisitor.VisitFuncExpression<MyModel>(x => Func.Coalesce(x.Name, "A", "B"));
      Utils.AssertExpression(exp2, @"COALESCE(""name"", 'A', 'B')");
    }

    // JSON
    [Fact]
    public void ExpressionJsonModelFunc()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.JsonModel.Name.Substring(1, 2));

      Utils.AssertExpression(exp, @"substring(((json_model->'name')::text) from 1 for 2)");
    }

    [Fact]
    public void ExpressionJsonSubModelFunc()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.JsonModel.MySubsubmodel.Name.Substring(1, 2));

      Utils.AssertExpression(exp, @"substring(((json_model->'inner'->'name')::text) from 1 for 2)");
    }

    // Functions
    [Fact]
    public void ExpressionArrayLength()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Binary.Length == 3 &&
                                                              x.ListString.Count == 4 &&
                                                              x.Name.Length == 1);

      Utils.AssertExpression(exp, @"(((octet_length(""binary"")) = (3)) AND ((array_length(list_string,1)) = (4))) AND ((LENGTH(""name"")) = (1))");
    }
  }
}