using System;
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

      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.Name == s1);
      var q = builder.Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.Eq(x => x.Name, s1);

      Utils.AssertRawQuery(q, b,
                           @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""name"") = (@1::text))",
                           new Param(s1, NpgsqlDbType.Text));
    }

    [Fact]
    public void WhereString()
    {
      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.Name == "name");
      var q = builder.Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.Eq(x => x.Name, "name");

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""name"") = ('name'))");
    }

    [Fact]
    public void WhereNumber()
    {
      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.Id == 2);
      var q = builder.Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.Eq(x => x.Id, 2);

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""id"") = (2))");
    }

    // Logic
    [Fact]
    public void WhereAndMultiple()
    {
      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.Id == 1);
      builder.Where(x => x.Name == "test");
      var q = builder.Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""id"") = (1)) AND ((""name"") = ('test'))");
    }

    [Fact]
    public void WhereAnd()
    {
      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.Id == 1 && x.Name == "test");
      var q = builder.Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.And(
          WhereBuilder<MyModel>.Eq(x => x.Id, 1),
          WhereBuilder<MyModel>.Eq(x => x.Name, "test"));

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".""model"" WHERE (((""id"") = (1)) AND ((""name"") = ('test')))");
    }

    [Fact]
    public void WhereOr()
    {
      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.Id == 1 || x.Name == "test");
      var q = builder.Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.Or(
          WhereBuilder<MyModel>.Eq(x => x.Id, 1),
          WhereBuilder<MyModel>.Eq(x => x.Name, "test"));

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".""model"" WHERE (((""id"") = (1)) OR ((""name"") = ('test')))");
    }

    [Fact]
    public void WhereBinaryMultiply()
    {
      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.Id * x.Id == 1);
      var q = builder.Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE (((""id"") * (""id"")) = (1))");
    }

    [Fact]
    public void WhereBinaryAddNumbers()
    {
      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.Id + x.Id == 1);
      var q = builder.Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE (((""id"") + (""id"")) = (1))");
    }

    [Fact]
    public void WhereBinaryAddStrings()
    {
      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.Name + x.Name == "X");
      var q = builder.Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE (((""name"") || (""name"")) = ('X'))");
    }

    [Fact]
    public void WhereBinarySubtract()
    {
      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.Id - x.Id == 1);
      var q = builder.Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE (((""id"") - (""id"")) = (1))");
    }

    [Fact]
    public void WhereBinaryGreaterThan()
    {
      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.Id > 1);
      var q = builder.Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""id"") > (1))");
    }

    [Fact]
    public void WhereBinaryGreaterThanEquals()
    {
      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.Id >= 1);
      var q = builder.Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""id"") >= (1))");
    }

    [Fact]
    public void WhereBinaryLessThan()
    {
      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.Id < 1);
      var q = builder.Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""id"") < (1))");
    }

    [Fact]
    public void WhereBinaryLessThanEquals()
    {
      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.Id <= 1);
      var q = builder.Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""id"") <= (1))");
    }

    // Operators
    [Fact]
    public void WhereOperatorIn()
    {
      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.Name.PgIn("A", "B"));
      var q = builder.Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.In(x => x.Name, new[] { "A", "B" });

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((""name"") IN (('A'),('B')))");
    }

    [Fact]
    public void WhereOperatorContainsAny()
    {
      var builder = new QueryBuilder<MyModel>();
      builder.Where(x => x.ListString.PgContainsAny("A", "B"));
      var q = builder.Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.ContainsAny(x => x.ListString, new[] { "A", "B" });

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".""model"" WHERE ((@1::text[]) && (""list_string""))",
                           new Param(new[] { "A", "B" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }
  }
}