using System.Threading.Tasks;
using KDPgDriver.Utils;
using Xunit;

namespace KDPgDriver.Tests.FunctionalTests
{
  public class Test
  {
    static Test()
    {
      MyInit.Init();
    }

    private async Task<Driver> CreateDriver()
    {
      var dr = new Driver("postgresql://test:test@localhost:5432/kdpgdriver_test", "public");
      await dr.InitializeAsync();

      await dr.QueryRawAsync(@"
DROP TABLE IF EXISTS model;
DROP TYPE IF EXISTS enum;
DROP TYPE IF EXISTS enum2;

CREATE TYPE enum AS ENUM ('A', 'B', 'C');
CREATE TYPE enum2 AS ENUM ('A', 'B', 'C');

CREATE TABLE model (
  id int PRIMARY KEY,
  name text,
  list_string text[],
  list_string2 text[],
  enum enum,
  enum2 enum2,
  list_enum enum[],
  datetime timestamp,
  json_object1 jsonb,
  json_model jsonb,
  json_array1 jsonb
);

INSERT INTO model(id, name, list_string, enum, list_enum) VALUES(1, 'test1', '{a,b,c}', 'A', '{A}');
INSERT INTO model(id, name, list_string, enum, list_enum) VALUES(2, 'test2', '{a,b}', 'B', '{B}');
INSERT INTO model(id, name, list_string, enum, list_enum) VALUES(3, 'test3', '{a}', 'C', '{B,C}');
");

      return dr;
    }

    [Fact]
    public async Task WhereEnumFetch()
    {
      var dr = await CreateDriver();

      var res = await dr.QueryGetAllAsync(Builders<MyModel>.Query.Select(x => new {
          x.Enum
      }));

      Assert.Collection(res,
                        item => { Assert.Equal(MyEnum.A, item.Enum); },
                        item => { Assert.Equal(MyEnum.B, item.Enum); },
                        item => { Assert.Equal(MyEnum.C, item.Enum); });
    }

    [Fact]
    public async Task WhereEnumArrayFetch()
    {
      var dr = await CreateDriver();

      var res = await dr.QueryGetAllAsync(Builders<MyModel>.Query.Select(x => new {
          x.ListEnum
      }));

      Assert.Collection(res,
                        item => { Assert.Collection(item.ListEnum, x => { Assert.Equal(MyEnum.A, x); }); },
                        item => { Assert.Collection(item.ListEnum, x => { Assert.Equal(MyEnum.B, x); }); },
                        item =>
                        {
                          Assert.Collection(item.ListEnum,
                                            x => { Assert.Equal(MyEnum.B, x); },
                                            x => { Assert.Equal(MyEnum.C, x); });
                        });
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

        Assert.Collection(item.ListEnum,
                          subitem => { Assert.Equal(MyEnum.B, subitem); });
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

    [Fact]
    public async Task Test1()
    {
      var dr = await CreateDriver();

      var res = await dr.From<MyModel>()
                        .Select()
                        .Where(x => x.Id == 2)
                        .OrderBy(x => x.DateTime)
                        .ToListAsync();

      Assert.Collection(res, item =>
      {
        Assert.Equal(2, item.Id);
        Assert.Equal("test2", item.Name);

        Assert.Collection(item.ListString,
                          subitem => { Assert.Equal("a", subitem); },
                          subitem => { Assert.Equal("b", subitem); });

        Assert.Collection(item.ListEnum,
                          subitem => { Assert.Equal(MyEnum.B, subitem); });
      });
    }

    [Fact]
    public async Task TestJson1()
    {
      var dr = await CreateDriver();

      var res = await dr.From<MyModel>()
                        .Select(x => (bool?) (x.JsonModel.MySubsubmodel.Number == 2))
                        .ToListAsync();

      Assert.Equal(3, res.Count);
    }

    [Fact]
    public async Task TestDriverBatch()
    {
      var dr = await CreateDriver();

      var b = dr.CreateBatch();

      var task1 = b.From<MyModel>().Select(x => x.Id).Where(x => x.Id == 1).ToListAsync();
      var task2 = b.From<MyModel>().Select(x => x.Id).Where(x => x.Id == 2).ToListAsync();

      await b.Execute();

      Assert.Collection(task1.Result, x => Assert.Equal(1, x));
      Assert.Collection(task2.Result, x => Assert.Equal(2, x));
    }

    [Fact]
    public async Task TestTransactionBatch()
    {
      var dr = await CreateDriver();

      // transaction rolled back
      using (var tr = await dr.CreateTransaction()) {
         var b = tr.CreateBatch();
         var task1 = b.QueryAsync(Builders<MyModel>.Insert.UseField(x => x.Id).AddObject(new MyModel() { Id = 10 }));
         var task2 = b.QueryAsync(Builders<MyModel>.Insert.UseField(x => x.Id).AddObject(new MyModel() { Id = 11 }));
         await b.Execute();
      }

      var res1 = await dr.From<MyModel>().Select(x => x.Id).ToListAsync();
      Assert.Equal(3, res1.Count);
      
      // transaction commited
      using (var tr = await dr.CreateTransaction()) {
        var b = tr.CreateBatch();
        var task1 = b.QueryAsync(Builders<MyModel>.Insert.UseField(x => x.Id).AddObject(new MyModel() { Id = 10 }));
        var task2 = b.QueryAsync(Builders<MyModel>.Insert.UseField(x => x.Id).AddObject(new MyModel() { Id = 11 }));
        await b.Execute();

        await tr.CommitAsync();
      }

      var res2 = await dr.From<MyModel>().Select(x => x.Id).ToListAsync();
      Assert.Equal(5, res2.Count);
    }
  }
}