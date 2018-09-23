using System.Threading.Tasks;
using Xunit;

namespace KDPgDriver.Tests.FunctionalTests
{
  public partial class Test
  {
    [Fact]
    public async Task TestJoin()
    {
      var dr = await CreateDriver();

      var rows = await dr.FromMany<MyModel, MyModel2>((model, model2) => model.Id == model2.ModelId)
                         .Map((a, b) => new {
                             M1 = a,
                             M2 = b,
                         })
                         .Select()
                         .ToListAsync();

      Assert.Collection(rows,
                        item =>
                        {
                          Assert.Equal(1, item.M1.Id);
                          Assert.Equal(1, item.M2.Id);
                        },
                        item =>
                        {
                          Assert.Equal(1, item.M1.Id);
                          Assert.Equal(2, item.M2.Id);
                        },
                        item =>
                        {
                          Assert.Equal(2, item.M1.Id);
                          Assert.Equal(3, item.M2.Id);
                        },
                        item =>
                        {
                          Assert.Equal(3, item.M1.Id);
                          Assert.Null(item.M2.Name1);
                        });
    }

    [Fact]
    public async Task TestJoinAnonymous()
    {
      var dr = await CreateDriver();

      var rows = await dr.FromMany<MyModel, MyModel2>((model, model2) => model.Id == model2.ModelId)
                         .Map((a, b) => new {
                             M1 = a,
                             M2 = b,
                         })
                         .Select(x => new {
                             M1 = x.M1,
                             M2_name = x.M2.Name1,
                             M2_id = x.M2.Id * 2,
                         })
                         .ToListAsync();

      Assert.Collection(rows,
                        item =>
                        {
                          Assert.Equal(1, item.M1.Id);
                          Assert.Equal("subtest1", item.M2_name);
                          Assert.Equal(2, item.M2_id);
                        },
                        item =>
                        {
                          Assert.Equal(1, item.M1.Id);
                          Assert.Equal("subtest2", item.M2_name);
                          Assert.Equal(4, item.M2_id);
                        },
                        item =>
                        {
                          Assert.Equal(2, item.M1.Id);
                          Assert.Equal("subtest3", item.M2_name);
                          Assert.Equal(6, item.M2_id);
                        },
                        item =>
                        {
                          Assert.Equal(3, item.M1.Id);
                          Assert.Null(item.M2_name);
                          Assert.Equal(0, item.M2_id);
                        });
    }

    [Fact]
    public async Task TestJoinReturnSingle()
    {
      var dr = await CreateDriver();

      var rows = await dr.FromMany<MyModel, MyModel2>((model, model2) => model.Id == model2.ModelId)
                         .Map((a, b) => new {
                             M1 = a,
                             M2 = b,
                         })
                         .Select(x => x.M1)
                         .ToListAsync();

      Assert.Collection(rows,
                        item => Assert.Equal(1, item.Id),
                        item => Assert.Equal(1, item.Id),
                        item => Assert.Equal(2, item.Id),
                        item => Assert.Equal(3, item.Id));
    }
  }
}