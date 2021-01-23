using System;
using System.Threading.Tasks;
using Xunit;

namespace KDPgDriver.Tests.FunctionalTests
{
  public partial class Test
  {
    [Fact]
    public async Task TestDate()
    {
      var dr = await CreateDriver();

      var date = DateTime.Parse("2018-01-01 12:34");

      await dr.Update<MyModel>()
              .Where(x => x.Id == 1)
              .SetField(x => x.Date, date)
              .ExecuteAsync();

      var res = await dr.From<MyModel>()
                        .Select()
                        .Where(x => x.Id == 1)
                        .ToSingleAsync();

      Assert.Equal(date.Date, res.Date);
    }

    [Fact]
    public async Task TestTime()
    {
      var dr = await CreateDriver();

      var date = DateTime.Parse("2018-01-01 12:34");

      await dr.Update<MyModel>()
              .Where(x => x.Id == 1)
              .SetField(x => x.Time, date.TimeOfDay)
              .ExecuteAsync();

      var res = await dr.From<MyModel>()
                        .Select()
                        .Where(x => x.Id == 1)
                        .ToSingleAsync();

      Assert.Equal(date.TimeOfDay, res.Time);
    }

    [Fact]
    public async Task TestDateTime()
    {
      var dr = await CreateDriver();

      var date = DateTime.Parse("2018-01-01 12:34");

      await dr.Update<MyModel>()
              .Where(x => x.Id == 1)
              .SetField(x => x.DateTime, date)
              .ExecuteAsync();

      var res = await dr.From<MyModel>()
                        .Select()
                        .Where(x => x.Id == 1)
                        .ToSingleAsync();

      Assert.Equal(date, res.DateTime);
    }
  }
}