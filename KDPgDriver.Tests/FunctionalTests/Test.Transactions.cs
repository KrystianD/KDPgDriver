using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace KDPgDriver.Tests.FunctionalTests
{
  public partial class Test
  {
    [Fact]
    public async Task TestTransaction()
    {
      var dr = await CreateDriver();

      // transaction rolled back
      using (var tr = await dr.CreateTransaction()) {
        var b = tr.CreateBatch();
        var task1 = b.Insert<MyModel>().UseField(x => x.Id).AddObject(new MyModel() { Id = 10 }).ExecuteAsync();
        var task2 = b.Insert<MyModel>().UseField(x => x.Id).AddObject(new MyModel() { Id = 11 }).ExecuteAsync();
        await b.Execute();
      }

      var res1 = await dr.From<MyModel>().Select(x => x.Id).ToListAsync();
      Assert.Equal(3, res1.Count);

      // transaction committed
      using (var tr = await dr.CreateTransaction()) {
        var b = tr.CreateBatch();
        var task1 = b.Insert<MyModel>().UseField(x => x.Id).AddObject(new MyModel() { Id = 10 }).ExecuteAsync();
        var task2 = b.Insert<MyModel>().UseField(x => x.Id).AddObject(new MyModel() { Id = 11 }).ExecuteAsync();
        await b.Execute();

        await tr.CommitAsync();
      }

      var res2 = await dr.From<MyModel>().Select(x => x.Id).ToListAsync();
      Assert.Equal(5, res2.Count);
    }

    [Fact]
    public async Task TestTransactionBatch()
    {
      var dr = await CreateDriver();

      // transaction failed back
      {
        var b = dr.CreateTransactionBatch();
        var task1 = b.Insert<MyModel>().UseField(x => x.Id).AddObject(new MyModel() { Id = 10 }).ExecuteAsync();
        var task2 = b.Insert<MyModel>().UseField(x => x.Id).AddObject(new MyModel() { Id = 3 }).ExecuteAsync();
        await Assert.ThrowsAsync<Npgsql.PostgresException>(async () => {
          await b.Execute();
        });
      }

      var res1 = await dr.From<MyModel>().Select(x => x.Id).ToListAsync();
      Assert.Equal(3, res1.Count);

      // transaction committed
      {
        var b = dr.CreateTransactionBatch();
        var task1 = b.Insert<MyModel>().UseField(x => x.Id).AddObject(new MyModel() { Id = 10 }).ExecuteAsync();
        var task2 = b.Insert<MyModel>().UseField(x => x.Id).AddObject(new MyModel() { Id = 11 }).ExecuteAsync();
        await b.Execute();
      }

      var res2 = await dr.From<MyModel>().Select(x => x.Id).ToListAsync();
      Assert.Equal(5, res2.Count);
    }

    [Fact]
    public async Task TestTransactionBatchMixed()
    {
      var dr = await CreateDriver();

      var t = await dr.CreateTransaction();

      var b = t.CreateBatch();
      b.Insert<MyModel>().UseField(x => x.Id).AddObject(new MyModel() { Id = 10 }).Schedule();
      var selectTask = b.From<MyModel>().Select(x => x.Id).ToListAsync();

      await b.Execute();
      await t.CommitAsync();

      Assert.Collection(selectTask.Result,
                        x => Assert.Equal(1, x),
                        x => Assert.Equal(2, x),
                        x => Assert.Equal(3, x),
                        x => Assert.Equal(10, x));
    }

    [Fact]
    public async Task TestTransactionBatchError()
    {
      var dr = await CreateDriver();

      using (var t = await dr.CreateTransaction()) {
        var b = t.CreateBatch();
        b.Insert<MyModelLink1>().UseField(x => x.Id).AddObject(new MyModelLink1() { Id = 2 }).Schedule();
        b.Delete<MyModelLink1>().Where(x => x.Id == 1).Schedule();

        await Assert.ThrowsAsync<PostgresException>(() => b.Execute());
        
        await t.CommitAsync();
      }
    }
  }
}