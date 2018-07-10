using System;
using System.Collections.Generic;
using KDPgDriver.Builder;
using KDPgDriver.Utils;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests
{
  public class InsertQuery
  {
    static InsertQuery()
    {
      MyInit.Init();
    }

    [Fact]
    public void InsertSingle()
    {
      var obj = new MyModel {
          Id = 4
      };

      var q = Builders<MyModel>.Insert
                               .UseField(x => x.Id)
                               .AddObject(obj);

      Utils.AssertRawQuery(q, @"INSERT INTO ""public"".""model""(""id"") VALUES (4) RETURNING ""id""");
    }

    [Fact]
    public void InsertMany()
    {
      var objs = new List<MyModel>() {
          new MyModel() {
              Id = 1,
              Name = "A",
          },
          new MyModel() {
              Id = 2,
              Name = "B",
          },
      };

      var q = Builders<MyModel>.Insert
                               .UseField(x => x.Id)
                               .UseField(x => x.Name)
                               .AddMany(objs);

      Utils.AssertRawQuery(q, @"INSERT INTO ""public"".""model""(""id"",""name"") VALUES (1,'A'),(2,'B') RETURNING ""id""");
    }

    [Fact]
    public void InsertList()
    {
      var obj = new MyModel {
          ListEnum = new List<MyEnum>() { MyEnum.A, MyEnum.C },
          ListString = new List<string>() { "A", "B" },
      };

      var q = Builders<MyModel>.Insert
                               .UseField(x => x.ListEnum)
                               .UseField(x => x.ListString)
                               .AddObject(obj);

      Utils.AssertRawQuery(q, @"INSERT INTO ""public"".""model""(""list_enum"",""list_string"") VALUES (@1::enum[],@2::text[]) RETURNING ""id""",
                           new Param(new[] { "A", "C" }, NpgsqlDbType.Array | NpgsqlDbType.Text),
                           new Param(new[] { "A", "B" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }
  }
}