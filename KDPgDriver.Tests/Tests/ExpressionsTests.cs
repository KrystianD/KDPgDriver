using System;
using System.Collections.Generic;
using System.Linq;
using KDPgDriver.Builder;
using KDPgDriver.Utils;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests
{
  public class ExpressionsTests
  {
    static ExpressionsTests()
    {
      MyInit.Init();
    }

    // Logic and binary operators

    #region Operator Add

    [Fact]
    public void ExpressionAddNumbers()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id + x.Id == 1);

      Utils.AssertExpression(exp, @"((id) + (id)) = (1)");
    }

    [Fact]
    public void ExpressionAddStrings()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name + x.Name == "X");

      Utils.AssertExpression(exp, @"((name) || (name)) = ('X')");
    }

    #endregion

    [Fact]
    public void ExpressionEq()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id == 2);

      Utils.AssertExpression(exp, @"(id) = (2)");
    }

    [Fact]
    public void ExpressionEqLongString()
    {
      string s1 = "long string long string long string long string long string long string long string";

      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name == s1);

      Utils.AssertExpression(exp, @"(name) = (@1::text)",
                             new Param(s1, NpgsqlDbType.Text));
    }

    [Fact]
    public void ExpressionMultiply()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id * x.Id == 1);

      Utils.AssertExpression(exp, @"((id) * (id)) = (1)");
    }

    [Fact]
    public void ExpressionSubtract()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id - x.Id == 1);

      Utils.AssertExpression(exp, @"((id) - (id)) = (1)");
    }

    [Fact]
    public void ExpressionGreaterThan()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id > 1);

      Utils.AssertExpression(exp, @"(id) > (1)");
    }

    [Fact]
    public void ExpressionGreaterThanEquals()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id >= 1);

      Utils.AssertExpression(exp, @"(id) >= (1)");
    }

    [Fact]
    public void ExpressionLessThan()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id < 1);

      Utils.AssertExpression(exp, @"(id) < (1)");
    }

    [Fact]
    public void ExpressionLessThanEquals()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id <= 1);

      Utils.AssertExpression(exp, @"(id) <= (1)");
    }

    [Fact]
    public void ExpressionOr()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id == 1 || x.Id == 2);

      Utils.AssertExpression(exp, @"((id) = (1)) OR ((id) = (2))");
    }

    [Fact]
    public void ExpressionAnd()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Id == 1 && x.Id == 2);

      Utils.AssertExpression(exp, @"((id) = (1)) AND ((id) = (2))");
    }

    // Functions
    [Fact]
    public void ExpressionSubstring()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.Substring(5 + x.Id, 10) == "A");

      Utils.AssertExpression(exp, @"(substring((name) from (5) + (id) for 10)) = ('A')");
    }

    #region Operator In

    [Fact]
    public void ExpressionInDirect()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.PgIn("A", "B"));

      Utils.AssertExpression(exp, @"(name) = ANY(@1::text[])",
                             new Param(new[] { "A", "B" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    [Fact]
    public void ExpressionInArray()
    {
      var items = new[] { null, "A1", "A2", "B3", "A4" };

      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.PgIn(items));

      Utils.AssertExpression(exp, @"(name) = ANY(@1::text[])",
                             new Param(new[] { null, "A1", "A2", "B3", "A4" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    [Fact]
    public void ExpressionInList()
    {
      var items = new List<string>() { null, "A1", "A2", "B3", "A4" };

      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.PgIn(items));

      Utils.AssertExpression(exp, @"(name) = ANY(@1::text[])",
                             new Param(new[] { null, "A1", "A2", "B3", "A4" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    [Fact]
    public void ExpressionInEnumerable()
    {
      var items = new List<string>() { null, "A1", "A2", "B3", "A4" };
      var items2 = items.Where(x => x == null || x.StartsWith("A")).Distinct();

      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.PgIn(items2));

      Utils.AssertExpression(exp, @"(name) = ANY(@1::text[])",
                             new Param(new[] { null, "A1", "A2", "A4" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
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

      Utils.AssertExpression(exp, @"(name) LIKE ('%' || kdpg_escape_like('A') || '%')");
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

      Utils.AssertExpression(exp, @"(name) LIKE ('%' || kdpg_escape_like('A') || '%')");
    }

    [Fact]
    public void ExpressionStringILike()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.PgILike("A"));

      Utils.AssertExpression(exp, @"(name) ILIKE ('%' || kdpg_escape_like('A') || '%')");
    }

    [Fact]
    public void ExpressionStartsWith()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Name.StartsWith("A"));

      Utils.AssertExpression(exp, @"(name) LIKE (kdpg_escape_like('A') || '%')");
    }

    // Data types
    [Fact]
    public void ExpressionDateTime()
    {
      var date = DateTime.Parse("2018-01-01 12:34");
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.DateTime == date);

      Utils.AssertExpression(exp, @"(datetime) = (@1::timestamp)",
                             new Param(date, NpgsqlDbType.Timestamp));
    }
  }
}