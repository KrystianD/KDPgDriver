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
      var obj = new MyModel
      {
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
      var objs = new List<MyModel>()
      {
          new MyModel()
          {
              Id = 1,
          },
          new MyModel()
          {
              Id = 2,
          },
      };

      var q = Builders<MyModel>.Insert
                               .UseField(x => x.Id)
                               .AddMany(objs);

      Utils.AssertRawQuery(q, @"INSERT INTO ""public"".""model""(""id"") VALUES (1),(2) RETURNING ""id""");
    }
  }
}