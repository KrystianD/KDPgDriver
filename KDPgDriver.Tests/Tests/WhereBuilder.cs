using System;
using System.Collections.Generic;
using System.Linq;
using KDPgDriver.Builder;
using KDPgDriver.Utils;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests
{
  public class WhereBuilderUnitTests
  {
    // Data types
    [Fact]
    public void WhereLongString()
    {
      string s1 = "long string long string long string long string long string long string long string";

      var q = Builders<MyModel>.Query
                               .Where(x => x.Name == s1)
                               .Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.Eq(x => x.Name, s1);

      Utils.AssertRawQuery(q, b,
                           @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""name"") = (@1::text))",
                           new Param(s1, NpgsqlDbType.Text));
    }

    [Fact]
    public void WhereString()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Name == "name")
                               .Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.Eq(x => x.Name, "name");

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""name"") = ('name'))");
    }

    [Fact]
    public void WhereNumber()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Id == 2)
                               .Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.Eq(x => x.Id, 2);

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""id"") = (2))");
    }

    // Logic
    [Fact]
    public void WhereAndMultiple()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Id == 1)
                               .Where(x => x.Name == "test")
                               .Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""id"") = (1)) AND ((""name"") = ('test'))");
    }

    [Fact]
    public void WhereAnd()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Id == 1 && x.Name == "test")
                               .Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.And(
          WhereBuilder<MyModel>.Eq(x => x.Id, 1),
          WhereBuilder<MyModel>.Eq(x => x.Name, "test"));

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".""model"" WHERE (((""id"") = (1)) AND ((""name"") = ('test')))");
    }

    [Fact]
    public void WhereOr()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Id == 1 || x.Name == "test")
                               .Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.Or(
          WhereBuilder<MyModel>.Eq(x => x.Id, 1),
          WhereBuilder<MyModel>.Eq(x => x.Name, "test"));

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".""model"" WHERE (((""id"") = (1)) OR ((""name"") = ('test')))");
    }

    [Fact]
    public void WhereBinaryMultiply()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Id * x.Id == 1)
                               .Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE (((""id"") * (""id"")) = (1))");
    }

    [Fact]
    public void WhereBinaryAddNumbers()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Id + x.Id == 1)
                               .Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE (((""id"") + (""id"")) = (1))");
    }

    [Fact]
    public void WhereBinaryAddStrings()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Name + x.Name == "X")
                               .Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE (((""name"") || (""name"")) = ('X'))");
    }

    [Fact]
    public void WhereBinarySubtract()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Id - x.Id == 1)
                               .Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE (((""id"") - (""id"")) = (1))");
    }

    [Fact]
    public void WhereBinaryGreaterThan()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Id > 1)
                               .Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""id"") > (1))");
    }

    [Fact]
    public void WhereBinaryGreaterThanEquals()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Id >= 1)
                               .Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""id"") >= (1))");
    }

    [Fact]
    public void WhereBinaryLessThan()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Id < 1)
                               .Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""id"") < (1))");
    }

    [Fact]
    public void WhereBinaryLessThanEquals()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Id <= 1)
                               .Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""id"") <= (1))");
    }

    // Operators
    [Fact]
    public void WhereOperatorInDirect()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Name.PgIn("A", "B"))
                               .Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.In(x => x.Name, new[] { "A", "B" });

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""name"") = ANY(@1::text[]))",
                           new Param(new[] { "A", "B" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    [Fact]
    public void WhereOperatorInArray()
    {
      var items = new[] { null, "A1", "A2", "B3", "A4" };

      var q = Builders<MyModel>.Query
                               .Where(x => x.Name.PgIn(items))
                               .Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.In(x => x.Name, items);

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""name"") = ANY(@1::text[]))",
                           new Param(new[] { null, "A1", "A2", "B3", "A4" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    [Fact]
    public void WhereOperatorInList()
    {
      var items = new List<string>() { null, "A1", "A2", "B3", "A4" };

      var q = Builders<MyModel>.Query
                               .Where(x => x.Name.PgIn(items))
                               .Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.In(x => x.Name, items);

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""name"") = ANY(@1::text[]))",
                           new Param(new[] { null, "A1", "A2", "B3", "A4" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    [Fact]
    public void WhereOperatorInEnumerable()
    {
      var items = new List<string>() { null, "A1", "A2", "B3", "A4" };
      var items2 = items.Where(x => x == null || x.StartsWith("A")).Distinct();

      var q = Builders<MyModel>.Query
                               .Where(x => x.Name.PgIn(items2))
                               .Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.In(x => x.Name, items2);

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""name"") = ANY(@1::text[]))",
                           new Param(new[] { null, "A1", "A2", "A4" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    [Fact]
    public void WhereOperatorContainsAny()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.ListString.PgContainsAny("A", "B"))
                               .Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.ContainsAny(x => x.ListString, new[] { "A", "B" });

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((@1::text[]) && (""list_string""))",
                           new Param(new[] { "A", "B" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }
  }
}