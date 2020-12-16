using System.Threading.Tasks;
using KDPgDriver.Utils;
using Xunit;

namespace KDPgDriver.Tests.FunctionalTests
{
  public partial class Test
  {
    [Fact]
    public async Task TestSelectSubquery()
    {
      var dr = await CreateDriver();

      var subq = Builders<MyModel>.Select(x => x.Id)
                                  .Where(x => x.Name.StartsWith("test"))
                                  .AsSubquery();

      var res = await dr.From<MyModel2>()
                        .Select(x => x.Name1)
                        .Where(x => x.Id.PgIn(subq))
                        .ToListAsync();

      Assert.Collection(res,
                        item => Assert.Equal("subtest1", item),
                        item => Assert.Equal("subtest2", item),
                        item => Assert.Equal("subtest3", item));
    }
  }
}