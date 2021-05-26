using System;
using System.Threading.Tasks;
using Xunit;

namespace KDPgDriver.Tests.FunctionalTests
{
  public partial class Test
  {
    [Fact]
    public async Task TestTimeout()
    {
      var dr = await CreateDriver();

      await Assert.ThrowsAsync<TimeoutException>(() => dr.QueryRawAsync("SELECT pg_sleep(5)", TimeSpan.FromSeconds(1)));
    }
  }
}