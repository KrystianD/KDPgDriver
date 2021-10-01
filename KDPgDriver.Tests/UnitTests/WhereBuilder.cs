using KDPgDriver.Builders;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests.UnitTests
{
  public class WhereBuilderUnitTests
  {
    static WhereBuilderUnitTests()
    {
      MyInit.Init();
    }

    [Fact]
    public void WhereBool()
    {
      var q = Builders<MyModel>.Select(x => x.Bool)
                               .Where(x => x.Bool);

      Utils.AssertRawQuery(q, @"SELECT bool FROM model WHERE bool");
    }

    [Fact]
    public void WhereNot()
    {
      var q = Builders<MyModel>.Select(x => new { x.Id })
                               .Where(x => !(x.Id == 1 && x.Name == "test"));

      var b = WhereBuilder<MyModel>.Not(WhereBuilder<MyModel>.And(
                                            WhereBuilder<MyModel>.FromExpression(x => x.Id == 1),
                                            WhereBuilder<MyModel>.Eq(x => x.Name, "test")));

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM model WHERE NOT(((""id"") = (1)) AND ((""name"") = ('test')))");
    }

    [Fact]
    public void WhereAnd()
    {
      var q = Builders<MyModel>.Select(x => new { x.Id })
                               .Where(x => x.Id == 1 && x.Name == "test");

      var b = WhereBuilder<MyModel>.And(
          WhereBuilder<MyModel>.FromExpression(x => x.Id == 1),
          WhereBuilder<MyModel>.Eq(x => x.Name, "test"));

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM model WHERE ((""id"") = (1)) AND ((""name"") = ('test'))");
    }

    [Fact]
    public void WhereOr()
    {
      var q = Builders<MyModel>.Select(x => new { x.Id })
                               .Where(x => x.Id == 1 || x.Name == "test");

      var b = WhereBuilder<MyModel>.Or(
          WhereBuilder<MyModel>.Eq(x => x.Id, 1),
          WhereBuilder<MyModel>.Eq(x => x.Name, "test"));

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM model WHERE ((""id"") = (1)) OR ((""name"") = ('test'))");
    }

    [Fact]
    public void WhereAndMultiple()
    {
      var q = Builders<MyModel>.Select(x => new { x.Id })
                               .Where(x => x.Id == 1)
                               .Where(x => x.Name == "test");

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM model WHERE ((""id"") = (1)) AND ((""name"") = ('test'))");
    }

    [Fact]
    public void WhereInArray()
    {
      var q = Builders<MyModel>.Select(x => new { x.Id })
                               .Where(WhereBuilder<MyModel>.In(x => x.Id, new[] { 1, 2, 3 }));

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM model WHERE (""id"") = ANY(@1::int[])",
                           new Param(new[] { 1, 2, 3 }, NpgsqlDbType.Array | NpgsqlDbType.Integer));
    }

    [Fact]
    public void WhereInSubquery()
    {
      var sq = Builders<MyModel>.Select(x => x.Id).AsSubquery();

      var q = Builders<MyModel>.Select(x => new { x.Id })
                               .Where(WhereBuilder<MyModel>.In(x => x.Id, sq));

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM model WHERE (""id"") IN (SELECT ""id"" FROM model)");
    }
  }
}