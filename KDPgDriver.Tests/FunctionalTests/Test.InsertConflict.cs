using System.Threading.Tasks;
using Xunit;

namespace KDPgDriver.Tests.FunctionalTests
{
  public partial class Test
  {
    [Fact]
    public async Task TestOnConflictDoNothingSuccess()
    {
      var dr = await CreateDriver();

      var ins = await dr.Insert<MyModel>(new MyModel() { Id = 10 })
                        .UseField(x => x.Id)
                        .OnConflictDoNothing()
                        .ExecuteAsync();

      Assert.True(ins.RowInserted);
      Assert.Equal(10, ins.LastInsertId);
    }

    [Fact]
    public async Task TestOnConflictDoNothingExisting()
    {
      var dr = await CreateDriver();

      var ins = await dr.Insert<MyModel>(new MyModel() { Id = 3 })
                        .UseField(x => x.Id)
                        .OnConflictDoNothing()
                        .ExecuteAsync();

      Assert.False(ins.RowInserted);
    }

    [Fact]
    public async Task TestOnConflictDoUpdate()
    {
      var dr = await CreateDriver();

      var ins = await dr.Insert<MyModel>(new MyModel() { Id = 3 })
                        .UseField(x => x.Id)
                        .OnConflictDoUpdate(x => x.AddField(y => y.Id),
                                            x => x.SetField(y => y.Name, "changed1"))
                        .ExecuteAsync();

      Assert.True(ins.RowInserted);
      Assert.Equal(3, ins.LastInsertId);

      var res = await dr.From<MyModel>()
                        .Select()
                        .Where(x => x.Id == 3)
                        .ToSingleAsync();
      
      Assert.Equal("changed1", res.Name);
    }
  }
}