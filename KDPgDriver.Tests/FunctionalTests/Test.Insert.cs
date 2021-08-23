using System.Threading.Tasks;
using Xunit;

namespace KDPgDriver.Tests.FunctionalTests
{
  public partial class Test
  {
    [Fact]
    public async Task TestInsert()
    {
      var dr = await CreateDriver();

      var obj = new MyModel() {
          Name = "new",
          Binary = new byte[] { 1, 2, 3 },
      };

      await dr.Insert<MyModel>()
              .AddObject(obj)
              .ExecuteAsync();

      var rows = await dr.From<MyModel>().Select().Where(x => x.Id == 4).ToListAsync();
      Assert.Collection(rows,
                        x => {
                          Assert.Equal(4, x.Id);
                          Assert.Equal("new", x.Name);
                          Assert.Equal(new byte[] { 1, 2, 3 }, x.Binary);
                        });
    }

    [Fact]
    public async Task TestInsertMany()
    {
      var dr = await CreateDriver();

      var obj1 = new MyModel() {
          Name = "new1",
      };

      var obj2 = new MyModel() {
          Name = "new2",
      };

      var newIds = await dr.Insert<MyModel>()
                           .AddObject(obj1)
                           .AddObject(obj2)
                           .ExecuteForIdsAsync();
      
      Assert.Collection(newIds,
                        x => Assert.Equal(4, x),
                        x => Assert.Equal(5, x));
    }

    [Fact]
    public async Task TestInsertRefField()
    {
      var dr = await CreateDriver();

      var obj = new MyModel() {
          Name = "new",
      };

      var obj2 = new MyModel2() {
          Name1 = "new",
      };

      var b = dr.CreateTransactionBatch();

      b.Insert(obj)
       .Schedule();

      b.Insert<MyModel2>()
       .AddObject(obj2)
       .AddObject(obj2)
       .AddObject(obj2)
       .UseField(x => x.Name1)
       .UsePreviousInsertId<MyModel>(x => x.ModelId, x => x.Id)
       .Schedule();

      await b.Execute();

      var rows = await dr.From<MyModel2>().Select(x => x.ModelId).ToListAsync();
      Assert.Collection(rows,
                        x => Assert.Equal(1, x),
                        x => Assert.Equal(1, x),
                        x => Assert.Equal(2, x),
                        x => Assert.Equal(4, x),
                        x => Assert.Equal(4, x),
                        x => Assert.Equal(4, x),
                        x => Assert.Equal(4, x));
    }
  }
}