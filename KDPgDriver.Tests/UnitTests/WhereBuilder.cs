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
    public void WhereAnd()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Id == 1 && x.Name == "test")
                               .Select(x => new { x.Id });

      var b = WhereBuilder<MyModel>.And(
          WhereBuilder<MyModel>.FromExpression(x => x.Id == 1),
          WhereBuilder<MyModel>.Eq(x => x.Name, "test"));

      Utils.AssertRawQuery(q, b, @"SELECT id FROM public.model WHERE ((id) = (1)) AND ((name) = ('test'))");
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

      Utils.AssertRawQuery(q, b, @"SELECT id FROM public.model WHERE ((id) = (1)) OR ((name) = ('test'))");
    }

    [Fact]
    public void WhereAndMultiple()
    {
      var q = Builders<MyModel>.Query
                               .Where(x => x.Id == 1)
                               .Where(x => x.Name == "test")
                               .Select(x => new { x.Id });

      Utils.AssertRawQuery(q, @"SELECT id FROM public.model WHERE ((id) = (1)) AND ((name) = ('test'))");
    }
  }
}