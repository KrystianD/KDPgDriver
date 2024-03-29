using System;
using System.Threading.Tasks;
using KDPgDriver.Builders;
using KDPgDriver.Utils;
using Npgsql;
using Xunit;

namespace KDPgDriver.Tests.FunctionalTests
{
  public partial class Test
  {
    static Test()
    {
      MyInit.Init();
    }

    private async Task<Driver> CreateDriver()
    {
      var connString = new NpgsqlConnectionStringBuilder {
          Database = "postgres",
          Username = "postgres",
          Password = "test",
          Host = "localhost",
          Port = 9999,
          ApplicationName = "test",
          Pooling = false,
          SearchPath = "kd",
      };

      using (var connection = new NpgsqlConnection(connString.ToString())) {
        await connection.OpenAsync();
        using (var t = connection.CreateCommand()) {
          t.CommandText = @"CREATE SCHEMA IF NOT EXISTS ""kd"";";
          await t.ExecuteNonQueryAsync();
        }
      }

      var dr = new Driver(connString);
      await dr.InitializeAsync();

      await dr.QueryRawAsync(@"
CREATE SCHEMA IF NOT EXISTS ""Schema1"";

DROP TABLE IF EXISTS model;
DROP TABLE IF EXISTS ""public"".model2;
DROP TABLE IF EXISTS model_link1 CASCADE;
DROP TABLE IF EXISTS model_link2 CASCADE;
DROP TABLE IF EXISTS model_nopk;
DROP TYPE IF EXISTS enum;
DROP TYPE IF EXISTS ""Schema1"".enum2;

CREATE TYPE enum AS ENUM ('A', 'B', 'C');
CREATE TYPE ""Schema1"".enum2 AS ENUM ('A', 'B', 'C');

CREATE TABLE model (
  id SERIAL PRIMARY KEY,
  name text,
  list_string text[],
  list_string2 text[],
  enum enum,
  enum2 ""Schema1"".enum2,
  enum_text text,
  list_enum enum[],
  date date,
  time time,
  datetime timestamp,
  json_object1 jsonb,
  json_model jsonb,
  json_array1 jsonb,
  bool bool,
  ""binary"" bytea,
  private_int int,
  val_f32 real,
  val_f64 double precision,
  int_nullable int          NULL
);

CREATE TABLE ""public"".model2 (
  id SERIAL PRIMARY KEY,
  name1 text,
  model_id int
);

INSERT INTO model(name, list_string, enum, list_enum, private_int, ""binary"") VALUES('test1', '{a,b,c}', 'A', '{A}', 1, 'bin1'); -- id: 1
INSERT INTO model(name, list_string, enum, list_enum, private_int, ""binary"") VALUES('test2', '{a,b}', 'B', '{B}', 2, 'bin22');  -- id: 2
INSERT INTO model(name, list_string, enum, list_enum, private_int, ""binary"") VALUES('test3', '{a}', 'C', '{B,C}', 3, 'bin333'); -- id: 3

INSERT INTO ""public"".model2(name1, model_id) VALUES('subtest1', 1); -- id: 1
INSERT INTO ""public"".model2(name1, model_id) VALUES('subtest2', 1); -- id: 2
INSERT INTO ""public"".model2(name1, model_id) VALUES('subtest3', 2); -- id: 3
INSERT INTO ""public"".model2(name1, model_id) VALUES('subtest4', 4); -- id: 4

CREATE TABLE model_link1 (
  id SERIAL PRIMARY KEY
);
CREATE TABLE model_link2 (
  id SERIAL PRIMARY KEY,
  link_id int REFERENCES model_link1(id)
);

INSERT INTO model_link1(id) VALUES(1);
INSERT INTO model_link2(id,link_id) VALUES(1,1);

CREATE TABLE model_nopk (
  name text
);
");

      return dr;
    }

    [Fact]
    public async Task CreateDriverTest()
    {
      var dr = await CreateDriver();
    }

    [Fact]
    public async Task SelectAnonymous()
    {
      var dr = await CreateDriver();

      var res = await dr.From<MyModel>()
                        .Select(x => new { x.Name, IdCalc = x.Id + 5 })
                        .ToListAsync();

      Assert.Collection(res,
                        item => {
                          Assert.Equal("test1", item.Name);
                          Assert.Equal(6, item.IdCalc);
                        },
                        item => {
                          Assert.Equal("test2", item.Name);
                          Assert.Equal(7, item.IdCalc);
                        },
                        item => {
                          Assert.Equal("test3", item.Name);
                          Assert.Equal(8, item.IdCalc);
                        });
    }

    [Fact]
    public async Task SelectSingle()
    {
      var dr = await CreateDriver();

      var res = await dr.From<MyModel>()
                        .Select(x => x.Name)
                        .ToListAsync();

      Assert.Collection(res,
                        item => Assert.Equal("test1", item),
                        item => Assert.Equal("test2", item),
                        item => Assert.Equal("test3", item));
    }

    [Fact]
    public async Task SelectOnly()
    {
      var dr = await CreateDriver();

      var res = await dr.From<MyModel>()
                        .SelectOnly(x => x.Name)
                        .ToListAsync();

      Assert.Collection(res,
                        item => Assert.Equal("test1", item.Name),
                        item => Assert.Equal("test2", item.Name),
                        item => Assert.Equal("test3", item.Name));
    }

    [Fact]
    public async Task Exists()
    {
      var dr = await CreateDriver();

      var res = await dr.From<MyModel>()
                        .Exists()
                        .ToSingleAsync();

      Assert.True(res);

      res = await dr.From<MyModel>()
                    .Exists()
                    .Where(x => x.Name == "not-existing")
                    .ToSingleAsync();

      Assert.False(res);
    }

    [Fact]
    public async Task WhereEnumFetch()
    {
      var dr = await CreateDriver();

      var res = await dr.From<MyModel>()
                        .Select(x => new { x.Enum })
                        .ToListAsync();

      Assert.Collection(res,
                        item => { Assert.Equal(MyEnum.A, item.Enum); },
                        item => { Assert.Equal(MyEnum.B, item.Enum); },
                        item => { Assert.Equal(MyEnum.C, item.Enum); });
    }

    [Fact]
    public async Task WhereEnumArrayFetch()
    {
      var dr = await CreateDriver();

      var res = await dr.From<MyModel>()
                        .Select(x => new { x.ListEnum })
                        .ToListAsync();

      Assert.Collection(res,
                        item => { Assert.Collection(item.ListEnum, x => { Assert.Equal(MyEnum.A, x); }); },
                        item => { Assert.Collection(item.ListEnum, x => { Assert.Equal(MyEnum.B, x); }); },
                        item => {
                          Assert.Collection(item.ListEnum,
                                            x => { Assert.Equal(MyEnum.B, x); },
                                            x => { Assert.Equal(MyEnum.C, x); });
                        });
    }

    [Fact]
    public async Task WhereSimpleTest()
    {
      var dr = await CreateDriver();

      var res = await dr.From<MyModel>()
                        .Select()
                        .Where(x => x.Id == 2)
                        .ToListAsync();

      Assert.Collection(res, item => {
        Assert.Equal(2, item.Id);
        Assert.Equal("test2", item.Name);

        Assert.Equal(2, item.PrivateInt);

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
      var res = await dr.From<MyModel>().Select().Where(x => x.Id.PgIn(a)).ToListAsync();

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

      Assert.Collection(res, item => {
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
    public async Task TestUpdate()
    {
      var dr = await CreateDriver();

      await dr.Update<MyModel>()
              .SetField(x => x.Name, "A2")
              .Where(x => x.Id == 1)
              .ExecuteAsync();

      var newName = await dr.From<MyModel>().Select(x => x.Name).Where(x => x.Id == 1).ToSingleAsync();
      Assert.Equal("A2", newName);
    }

    [Fact]
    public async Task TestUpdateCoalesce()
    {
      var dr = await CreateDriver();

      await dr.Update<MyModel>()
              .SetField(x => x.Name, x => Func.Coalesce(x.Name, "Q"))
              .SetField(x => x.DateTime, x => Func.Coalesce(x.DateTime, new DateTime(1, 1, 1)))
              .Where(x => x.Id == 1)
              .ExecuteAsync();

      var val = await dr.From<MyModel>().Select().Where(x => x.Id == 1).ToSingleAsync();
      Assert.Equal("test1", val.Name);
      Assert.Equal(new DateTime(1, 1, 1), val.DateTime);
    }

    [Fact]
    public async Task TestDelete()
    {
      var dr = await CreateDriver();

      var rows = await dr.From<MyModel>().Select(x => x.Name).Where(x => x.Id == 1).ToListAsync();
      Assert.Equal(1, rows.Count);

      await dr.Delete<MyModel>()
              .Where(x => x.Id == 1)
              .ExecuteAsync();

      rows = await dr.From<MyModel>().Select(x => x.Name).Where(x => x.Id == 1).ToListAsync();
      Assert.Equal(0, rows.Count);
    }

    [Fact]
    public async Task TestAddToArray()
    {
      var dr = await CreateDriver();

      await dr.Update<MyModel>()
              .AddToList(x => x.ListString, "c")
              .AddToList(x => x.ListString, "d")
              .ExecuteAsync();

      var d = await dr.From<MyModel>().Select(x => x.ListString).Where(x => x.Id == 1).ToSingleAsync();

      Assert.Collection(d,
                        x => Assert.Equal("a", x),
                        x => Assert.Equal("b", x),
                        x => Assert.Equal("c", x),
                        x => Assert.Equal("c", x),
                        x => Assert.Equal("d", x));
    }

    [Fact]
    public async Task TestAddToArrayDistinct()
    {
      var dr = await CreateDriver();

      await dr.Update<MyModel>()
              .AddToList(x => x.ListString, "c", UpdateAddToListFlags.Distinct)
              .AddToList(x => x.ListString, "d", UpdateAddToListFlags.Distinct)
              .ExecuteAsync();

      var d = await dr.From<MyModel>().Select(x => x.ListString).Where(x => x.Id == 1).ToSingleAsync();

      Assert.Collection(d,
                        x => Assert.Equal("a", x),
                        x => Assert.Equal("b", x),
                        x => Assert.Equal("c", x),
                        x => Assert.Equal("d", x));
    }

    [Fact]
    public async Task TestVariables()
    {
      var dr = await CreateDriver();

      var obj = new MyModel() { Name = "new" };

      // Transaction
      using (var tr = await dr.CreateTransaction()) {
        var insertId = await tr.Insert<MyModel>()
                               .AddObject(obj)
                               .IntoVariable("var1")
                               .ExecuteForIdAsync();

        var modelId = await tr.From<MyModel>()
                              .Select(x => x.Id)
                              .Where(x => x.Id == Func.GetVariableInt("var1"))
                              .ToSingleAsync();

        Assert.Equal(4, insertId);
        Assert.Equal(4, modelId);
      }

      // Batch
      {
        var b = dr.CreateBatch();

        b.Insert<MyModel>()
         .AddObject(obj)
         .IntoVariable("var2")
         .Schedule();

        var task = b.From<MyModel>()
                    .Select(x => x.Id)
                    .Where(x => x.Id == Func.GetVariableInt("var2"))
                    .ToSingleAsync();

        await b.Execute();

        var modelId = task.Result;

        Assert.Equal(5, modelId); // one record inserted above
      }
    }

    [Fact]
    public async Task TestAggregateFunc()
    {
      var dr = await CreateDriver();

      var d = await dr.From<MyModel>()
                      .Select(x => AggregateFunc.Max(x.Id))
                      .ToSingleAsync();

      Assert.Equal(3, d);
    }

    [Fact]
    public async Task SelectArrayLengths()
    {
      var dr = await CreateDriver();

      var res = await dr.From<MyModel>()
                        .Select(x => new {
                            A1 = x.Binary.Length,
                            A2 = x.ListString.Count,
                            A3 = x.Name.Length,
                        })
                        .ToListAsync();

      Assert.Collection(res,
                        item => {
                          Assert.Equal(4, item.A1);
                          Assert.Equal(3, item.A2);
                          Assert.Equal(5, item.A3);
                        },
                        item => {
                          Assert.Equal(5, item.A1);
                          Assert.Equal(2, item.A2);
                          Assert.Equal(5, item.A3);
                        },
                        item => {
                          Assert.Equal(6, item.A1);
                          Assert.Equal(1, item.A2);
                          Assert.Equal(5, item.A3);
                        });
    }
  }
}