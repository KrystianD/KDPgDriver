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

      var rows = await dr.FromMany<MyModel2, MyModel>((model2, model) => model2.ModelId == model.Id)
                         .Map((a, b) => new {
                             Model2 = a,
                             Model = b,
                         })
                         .Select()
                         .ToListAsync();

      Assert.Collection(rows,
                        item =>
                        {
                          Assert.Equal(1, item.Model2.Id);
                          Assert.Equal(1, item.Model.Id);
                        },
                        item =>
                        {
                          Assert.Equal(2, item.Model2.Id);
                          Assert.Equal(1, item.Model.Id);
                          Assert.Equal(1, item.Model2.ModelId);
                        },
                        item =>
                        {
                          Assert.Equal(3, item.Model2.Id);
                          Assert.Equal(2, item.Model.Id);
                          Assert.Equal(2, item.Model2.ModelId);
                        },
                        item =>
                        {
                          Assert.Equal(4, item.Model2.Id);
                          Assert.Null(item.Model);
                        });
    }

    [Fact]
    public async Task TestJoinAnonymous()
    {
      var dr = await CreateDriver();

      var rows = await dr.FromMany<MyModel2, MyModel>((model2, model) => model2.ModelId == model.Id)
                         .Map((a, b) => new {
                             Model2 = a,
                             Model = b,
                         })
                         .Select(x => new {
                             Model = x.Model,
                             Model2_name = x.Model2.Name1,
                             Model2_id = x.Model2.Id * 2,
                         })
                         .ToListAsync();

      Assert.Collection(rows,
                        item =>
                        {
                          Assert.Equal(1 * 2, item.Model2_id);
                          Assert.Equal("subtest1", item.Model2_name);
                          Assert.Equal(1, item.Model.Id);
                        },
                        item =>
                        {
                          Assert.Equal(2 * 2, item.Model2_id);
                          Assert.Equal("subtest2", item.Model2_name);
                          Assert.Equal(1, item.Model.Id);
                        },
                        item =>
                        {
                          Assert.Equal(3 * 2, item.Model2_id);
                          Assert.Equal("subtest3", item.Model2_name);
                          Assert.Equal(2, item.Model.Id);
                        },
                        item =>
                        {
                          Assert.Equal(4 * 2, item.Model2_id);
                          Assert.Equal("subtest4", item.Model2_name);
                          Assert.Null(item.Model);
                        });
    }

    [Fact]
    public async Task TestJoinReturnSingle()
    {
      var dr = await CreateDriver();

      var rows = await dr.FromMany<MyModel2, MyModel>((model2, model) => model2.ModelId == model.Id)
                         .Map((a, b) => new {
                             Model2 = a,
                             Model = b,
                         })
                         .Select(x => x.Model)
                         .ToListAsync();

      Assert.Collection(rows,
                        item => Assert.Equal(1, item.Id),
                        item => Assert.Equal(1, item.Id),
                        item => Assert.Equal(2, item.Id),
                        item => Assert.Null(item));
    }
  }
}