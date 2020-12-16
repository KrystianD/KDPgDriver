using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;

namespace KDPgDriver.Tests.FunctionalTests
{
  public partial class Test
  {
    [Fact]
    public async Task TestTransactionLevel()
    {
      var dr = await CreateDriver();

      using (var tr1 = await dr.CreateTransaction(KDPgIsolationLevel.RepeatableRead)) {
        using (var tr2 = await dr.CreateTransaction(KDPgIsolationLevel.RepeatableRead)) {
          await tr2.From<MyModel>().Select(x => x.Id).Where(x => x.Id == 1).ToListAsync();

          await tr1.Update<MyModel>().Where(x => x.Id == 1).SetField(x => x.Name, "A").ExecuteAsync();
          await tr1.CommitAsync();

          // should fail due to RepeatableRead isolation level
          await Assert.ThrowsAsync<Npgsql.PostgresException>(async () => {
            await tr2.Update<MyModel>().Where(x => x.Id == 1).SetField(x => x.Name, "B").ExecuteAsync();
            await tr2.CommitAsync();
          });
        }
      }

      var rows = await dr.From<MyModel>().Select().Where(x => x.Id == 1).ToListAsync();
      Assert.Collection(rows, x => Assert.Equal("A", x.Name));
    }
  }
}