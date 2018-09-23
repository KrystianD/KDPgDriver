using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Xunit;

namespace KDPgDriver.Tests.FunctionalTests
{
  public partial class Test
  {
    [Fact]
    public async Task TestJson1()
    {
      var dr = await CreateDriver();

      var res = await dr.From<MyModel>()
                        .Select(x => (bool?) (x.JsonModel.MySubsubmodel.Number == 2))
                        .ToListAsync();

      Assert.Equal(3, res.Count);
    }

    [Fact]
    public async Task TestJsonUpdate()
    {
      var dr = await CreateDriver();

      await dr.Update<MyModel>()
              .SetField(x => x.JsonModel, new MySubmodel())
              .SetField(x => x.JsonModel.JsonObject2, JObject.FromObject(new { b = 1 }))
              .SetField(x => x.JsonModel.JsonObject2["a"], "A")
              .Where(x => x.Id == 1)
              .ExecuteAsync();


      var res = await dr.From<MyModel>()
                        .Select()
                        .Where(x => x.Id == 1)
                        .ToListAsync();

      Assert.Collection(res,
                        x => Assert.Equal("A", x.JsonModel.JsonObject2["a"]));
    }

    [Fact]
    public async Task TestJsonArrayAdd()
    {
      var dr = await CreateDriver();

      var m = new MySubmodel() {
          JsonArray2 = new JArray(1, 2, 2),
      };

      await dr.Update<MyModel>()
              .SetField(x => x.JsonModel, m)
              .AddToList(x => x.JsonModel.JsonArray2, 3)
              .RemoveAllFromList(x => x.JsonModel.JsonArray2, 2)
              .Where(x => x.Id == 1)
              .ExecuteAsync();

      var res = await dr.From<MyModel>()
                        .Select()
                        .Where(x => x.Id == 1)
                        .ToListAsync();

      Assert.Collection(res,
                        x => Assert.Collection(x.JsonModel.JsonArray2,
                                               y => Assert.Equal(1, y),
                                               y => Assert.Equal(3, y)));
    }
  }
}