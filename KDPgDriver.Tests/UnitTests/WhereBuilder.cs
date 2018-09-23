using KDPgDriver.Builders;
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

      Utils.AssertRawQuery(q, @"SELECT bool FROM ""public"".model WHERE bool");
    }

    [Fact]
    public void WhereNot()
    {
      var q = Builders<MyModel>.Select(x => new { x.Id })
                               .Where(x => !(x.Id == 1 && x.Name == "test"));

      var b = WhereBuilder<MyModel>.Not(WhereBuilder<MyModel>.And(
                                            WhereBuilder<MyModel>.FromExpression(x => x.Id == 1),
                                            WhereBuilder<MyModel>.Eq(x => x.Name, "test")));

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".model WHERE NOT(((""id"") = (1)) AND ((""name"") = ('test')))");
    }

    [Fact]
    public void WhereAnd()
    {
      var q = Builders<MyModel>.Select(x => new { x.Id })
                               .Where(x => x.Id == 1 && x.Name == "test");

      var b = WhereBuilder<MyModel>.And(
          WhereBuilder<MyModel>.FromExpression(x => x.Id == 1),
          WhereBuilder<MyModel>.Eq(x => x.Name, "test"));

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".model WHERE ((""id"") = (1)) AND ((""name"") = ('test'))");
    }

    [Fact]
    public void WhereOr()
    {
      var q = Builders<MyModel>.Select(x => new { x.Id })
                               .Where(x => x.Id == 1 || x.Name == "test");

      var b = WhereBuilder<MyModel>.Or(
          WhereBuilder<MyModel>.Eq(x => x.Id, 1),
          WhereBuilder<MyModel>.Eq(x => x.Name, "test"));

      Utils.AssertRawQuery(q, b, @"SELECT ""id"" FROM ""public"".model WHERE ((""id"") = (1)) OR ((""name"") = ('test'))");
    }

    [Fact]
    public void WhereAndMultiple()
    {
      var q = Builders<MyModel>.Select(x => new { x.Id })
                               .Where(x => x.Id == 1)
                               .Where(x => x.Name == "test");

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".model WHERE ((""id"") = (1)) AND ((""name"") = ('test'))");
    }
  }
}