using System;
using System.Collections.Generic;
using KDPgDriver.Queries;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests.UnitTests.Queries
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

      var q = Builders<MyModel>.Insert()
                               .UseField(x => x.Id)
                               .AddObject(obj);

      Utils.AssertRawQuery(q, @"INSERT INTO ""public"".model(""id"") VALUES (4) RETURNING ""id""");
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

      var q = Builders<MyModel>.Insert()
                               .UseField(x => x.Id)
                               .UseField(x => x.Name)
                               .AddMany(objs);

      Utils.AssertRawQuery(q, @"INSERT INTO ""public"".model(""id"",""name"") VALUES (1,'A'),(2,'B') RETURNING ""id""");
    }

    [Fact]
    public void InsertManyEmpty()
    {
      var objs = new List<MyModel>();

      var q = Builders<MyModel>.Insert()
                               .UseField(x => x.Id)
                               .UseField(x => x.Name)
                               .AddMany(objs);

      Utils.AssertRawQuery(q, @"SELECT 0");
    }

    [Fact]
    public void InsertList()
    {
      var obj = new MyModel {
          ListEnum = new List<MyEnum>() { MyEnum.A, MyEnum.C },
          ListString = new List<string>() { "A", "B" },
      };

      var q = Builders<MyModel>.Insert()
                               .UseField(x => x.ListEnum)
                               .UseField(x => x.ListString)
                               .AddObject(obj);

      Utils.AssertRawQuery(q, @"INSERT INTO ""public"".model(list_enum,list_string) VALUES (@1::""enum""[],@2::text[]) RETURNING ""id""",
                           new Param(new[] { "A", "C" }, NpgsqlDbType.Array | NpgsqlDbType.Text),
                           new Param(new[] { "A", "B" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    [Fact]
    public void InsertEnumSchema()
    {
      var obj = new MyModel {
          Enum2 = MyEnum2.A,
      };

      var q = Builders<MyModel>.Insert()
                               .UseField(x => x.Enum2)
                               .AddObject(obj);

      Utils.AssertRawQuery(q, @"INSERT INTO ""public"".model(enum2) VALUES ('A') RETURNING ""id""");
    }

    [Fact]
    public void InsertDateTime()
    {
      var date = DateTime.Parse("2018-01-01 12:34");

      var obj = new MyModel {
          DateTime = date,
      };

      var q = Builders<MyModel>.Insert()
                               .UseField(x => x.DateTime)
                               .AddObject(obj);

      Utils.AssertRawQuery(q, @"INSERT INTO ""public"".model(datetime) VALUES (@1::timestamp) RETURNING ""id""",
                           new Param(date, NpgsqlDbType.Timestamp));
    }

    [Fact]
    public void InsertOnConflictDoNothing()
    {
      var obj = new MyModel {
          Id = 4
      };

      var q = Builders<MyModel>.Insert()
                               .UseField(x => x.Id)
                               .AddObject(obj)
                               .OnConflict(OnInsertConflict.DoNothing);

      Utils.AssertRawQuery(q, @"INSERT INTO ""public"".model(""id"") VALUES (4) ON CONFLICT DO NOTHING RETURNING ""id""");
    }

    [Fact]
    public void InsertEasy()
    {
      var obj = new MyModel {
          Name = "A"
      };

      var q = Builders<MyModel>.Insert(obj)
                               .UseField(x => x.Name);

      Utils.AssertRawQuery(q, @"INSERT INTO ""public"".model(""name"") VALUES ('A') RETURNING ""id""");
    }

    [Fact]
    public void InsertRefId()
    {
      var obj = new MyModel2 {
          Name1 = "A"
      };

      var q = Builders<MyModel2>.Insert(obj)
                                .UseField(x => x.Name1)
                                .UsePreviousInsertId<MyModel>(x => x.ModelId, x => x.Id);

      Utils.AssertRawQuery(q, @"INSERT INTO ""public"".model2(name1,model_id) VALUES ('A',currval(pg_get_serial_sequence('""public"".model','id'))) RETURNING ""id""");
    }
  }
}