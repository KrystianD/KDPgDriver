using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Newtonsoft.Json;
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
                        .Select(x => (bool?)(x.JsonModel.MySubsubmodel.Number == 2))
                        .ToListAsync();

      Assert.Equal(3, res.Count);
    }

    private async Task<MySubmodel> FillJson(Driver dr)
    {
      var submodel = new MySubsubmodel() {
          Name = "sub",
      };

      var model = new MySubmodel() {
          Name = "str",
          Number = 2,
          Decimal = 1.234M,
          MySubsubmodel = submodel,
          JsonObject2 = JObject.FromObject(new { a = 1 }),
          JsonArray2 = JArray.FromObject(new[] { 1, 2, 3 }),
      };

      await dr.Update<MyModel>()
              .SetField(x => x.JsonModel, model)
              .Where(x => x.Id == 1)
              .ExecuteAsync();

      return model;
    }

    [Fact]
    public async Task TestJsonFetch()
    {
      var dr = await CreateDriver();
      var model = await FillJson(dr);

      var res = await dr.From<MyModel>()
                        .Select(x => x.JsonModel)
                        .Where(x => x.Id == 1)
                        .ToSingleAsync();

      Assert.Equal(model.Name, res.Name);
      Assert.Equal(model.Number, res.Number);
      Assert.Equal(model.Decimal, res.Decimal);
      Assert.Equal(JsonConvert.SerializeObject(model.MySubsubmodel), JsonConvert.SerializeObject(res.MySubsubmodel));
      Assert.Equal(model.JsonObject2, res.JsonObject2);
      Assert.Equal(model.JsonArray2, res.JsonArray2);
    }

    [Fact]
    public async Task TestJsonFetchColumns()
    {
      var dr = await CreateDriver();
      var model = await FillJson(dr);

      var res = await dr.From<MyModel>()
                        .Select(x => new {
                            x.JsonModel.Name,
                            x.JsonModel.Number,
                            x.JsonModel.Decimal,
                        })
                        .Where(x => x.Id == 1)
                        .ToSingleAsync();
      
      Assert.Equal(model.Name, res.Name);
      Assert.Equal(model.Number, res.Number);
      Assert.Equal(model.Decimal, res.Decimal);
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