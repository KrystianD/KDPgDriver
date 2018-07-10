using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using KDLib;
using KDPgDriver.Builder;
using KDPgDriver.Utils;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests
{
  public class FuncTests
  {
    private async Task<Driver> CreateDriver()
    {
      var dr = new Driver("postgresql://test:test@localhost:5432/kdpgdriver_test", "public");
      await dr.InitializeAsync();

      await dr.QueryRawAsync("DROP TABLE IF EXISTS model");
      await dr.QueryRawAsync(@"CREATE TABLE model (
  id int PRIMARY KEY,
  name text,
  list_string text[],
  list_string2 text[]
)");
      await dr.QueryRawAsync(@"
INSERT INTO model(id, name, list_string) VALUES(1, 'test1', '{a,b,c}');
INSERT INTO model(id, name, list_string) VALUES(2, 'test2', '{a,b}');
INSERT INTO model(id, name, list_string) VALUES(3, 'test3', '{a}');");

      return dr;
    }

    [Fact]
    public async Task WhereSimpleTest()
    {
      var dr = await CreateDriver();

      var res = await dr.QueryGetAllAsync(Builders<MyModel>.Query.Where(x => x.Id == 2).Select());

      Assert.Collection(res, item =>
      {
        Assert.Equal(2, item.Id);
        Assert.Equal("test2", item.Name);

        Assert.Collection(item.ListString,
                          subitem => { Assert.Equal("a", subitem); },
                          subitem => { Assert.Equal("b", subitem); });
      });
    }

    [Fact]
    public async Task WhereTestPgIn()
    {
      var dr = await CreateDriver();

      var a = new[] { 2, 3 };
      var res = await dr.QueryGetAllAsync(Builders<MyModel>.Query.Where(x => x.Id.PgIn(a)).Select());

      Assert.Collection(res,
                        item => { Assert.Equal(2, item.Id); },
                        item => { Assert.Equal(3, item.Id); });
    }
  }
}