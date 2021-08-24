using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Xunit;

namespace KDPgDriver.Tests.FunctionalTests
{
  public partial class Test
  {
    [Fact]
    public async Task TestJsonToDecimal()
    {
      var dr = await CreateDriver();

      var obj = new MyModel() {
          JsonObject1 = JObject.FromObject(new {
              value = "1.23",
          }),
      };

      await dr.Insert<MyModel>().AddObject(obj).ExecuteAsync();

      var value = await dr.From<MyModel>()
                          .Select(x => (decimal)x.JsonObject1["value"])
                          .Where(x => x.JsonObject1 != null)
                          .ToSingleAsync();

      Assert.Equal(1.23m, value);
    }
  }
}