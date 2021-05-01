using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace KDPgDriver.Tests.FunctionalTests
{
  public partial class Test
  {
    [Fact]
    public async Task TestBatchMixed()
    {
      var dr = await CreateDriver();

      var b = dr.CreateBatch();

      b.Update<MyModel>().SetField(x => x.Name, "A").Schedule();

      b.Delete<MyModelNoPK>().Schedule();

      b.Update<MyModel>().SetField(x => x.Name, "A").Schedule();

      b.Insert<MyModel>().UseField(x => x.Id).AddObject(new MyModel() { Id = 10 }).Schedule();

      b.Insert<MyModelNoPK>().AddObject(new MyModelNoPK() { Name = "A" }).Schedule();

      var selectTask1 = b.From<MyModel>().Select(x => x.Id).ToListAsync();

      b.Update<MyModel>().SetField(x => x.Name, "A").Schedule();

      b.Insert<MyModel>().UseField(x => x.Id).AddObject(new MyModel() { Id = 20 }).Schedule();
      var selectTask2 = b.From<MyModel>().Select(x => x.Id).ToListAsync();

      await b.Execute();

      Assert.True(selectTask1.Result.SequenceEqual(new[] { 1, 2, 3, 10 }));
      Assert.True(selectTask2.Result.SequenceEqual(new[] { 1, 2, 3, 10, 20 }));
    }
  }
}