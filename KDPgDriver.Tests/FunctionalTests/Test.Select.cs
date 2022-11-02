using System.Threading.Tasks;
using Xunit;

namespace KDPgDriver.Tests.FunctionalTests
{
  public partial class Test
  {
    private class CustomDto
    {
      public int Id { get; set; }
      public MyModel M1 { get; set; }
    }

    [Fact]
    public async Task SelectCustomDto()
    {
      var dr = await CreateDriver();

      var res = await dr.From<MyModel>()
                        .Select(x => new CustomDto {
                            M1 = x,
                            Id = x.Id * 2,
                        })
                        .ToListAsync();

      Assert.Collection(res,
                        item => {
                          Assert.Equal(1, item.M1.Id);
                          Assert.Equal(2, item.Id);
                        },
                        item => {
                          Assert.Equal(2, item.M1.Id);
                          Assert.Equal(4, item.Id);
                        },
                        item => {
                          Assert.Equal(3, item.M1.Id);
                          Assert.Equal(6, item.Id);
                        });
    }

    [Fact]
    public async Task SelectCustomDtoJoin()
    {
      var dr = await CreateDriver();

      var res = await dr.FromMany<MyModel2, MyModel>((model2, model) => model2.ModelId == model.Id)
                        .Map((a, b) => new {
                            Model2 = a,
                            Model = b,
                        })
                        .Select(x => new CustomDto {
                            M1 = x.Model,
                            Id = x.Model2.Id * 2,
                        })
                        .ToListAsync();

      Assert.Collection(res,
                        item => {
                          Assert.Equal(1, item.M1.Id);
                          Assert.Equal(2, item.Id);
                        },
                        item => {
                          Assert.Equal(1, item.M1.Id);
                          Assert.Equal(4, item.Id);
                        },
                        item => {
                          Assert.Equal(2, item.M1.Id);
                          Assert.Equal(6, item.Id);
                        },
                        item => {
                          Assert.Null(item.M1);
                          Assert.Equal(8, item.Id);
                        });
    }
  }
}