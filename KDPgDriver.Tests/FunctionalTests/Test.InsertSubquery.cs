using System.Threading.Tasks;
using Xunit;

namespace KDPgDriver.Tests.FunctionalTests
{
  public partial class Test
  {
    [Fact]
    public async Task TestInsertSubquery()
    {
      var dr = await CreateDriver();

      var subq = Builders<MyModel>.Select(x => x.Name)
                                  .Where(x => x.Id == 1)
                                  .AsSubquery();


      var obj = new MyModel2 { };

      await dr.Insert(obj)
              .UseField(x => x.Name1, subq)
              .ExecuteAsync();

      var res = await dr.From<MyModel2>()
                        .Select(x => x.Name1)
                        .ToListAsync();

      Assert.Collection(res,
                        x => Assert.Equal("subtest1", x),
                        x => Assert.Equal("subtest2", x),
                        x => Assert.Equal("subtest3", x),
                        x => Assert.Equal("subtest4", x),
                        x => Assert.Equal("test1", x));
    }

    [Fact]
    public async Task TestInsertSubqueryFailMultipleRows()
    {
      var dr = await CreateDriver();

      var subq = Builders<MyModel>.Select(x => x.Name)
                                  .AsSubquery();


      var obj = new MyModel2 { };

      await Assert.ThrowsAsync<Npgsql.PostgresException>(() => dr.Insert(obj)
                                                                 .UseField(x => x.Name1, subq)
                                                                 .ExecuteAsync());
    }
  }
}