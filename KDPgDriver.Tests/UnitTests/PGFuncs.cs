using System;
using KDPgDriver.Builders;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests.UnitTests
{
  public class PGFuncs
  {
    static PGFuncs()
    {
      MyInit.Init();
    }

    [Fact]
    public void FuncMD5()
    {
      var q = Builders<MyModel>.Select(x => x.Id).Where(x => Func.MD5(x.Name) == "hex");

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM model WHERE (MD5(""name"")) = ('hex')");
    }

    [Fact]
    public void FuncCount()
    {
      var q = Builders<MyModel>.Select(x => Func.Count(x.Id));

      Utils.AssertRawQuery(q, @"SELECT COUNT(""id"") FROM model");
    }

    [Fact]
    public void FuncNow()
    {
      var q = Builders<MyModel>.Select(x => x.Id).Where(x => x.DateTime > Func.Now());

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM model WHERE (datetime) > (NOW())");
    }

    [Fact]
    public void FuncRaw()
    {
      var q = Builders<MyModel>.Select(x => x.Id).Where(x => x.DateTime > Func.Raw<DateTime>("NOW() + INTERVAL 2 SECONDS"));

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM model WHERE (datetime) > (NOW() + INTERVAL 2 SECONDS)");
    }

    [Fact]
    public void FuncGetVariable()
    {
      var q1 = Builders<MyModel>.Select(x => x.Id).Where(x => x.Id > Func.GetVariableInt("var1"));
      Utils.AssertRawQuery(q1, @"SELECT ""id"" FROM model WHERE (""id"") > (current_setting('vars.var1')::int)");

      var q2 = Builders<MyModel>.Select(x => x.Id).Where(x => x.Name == Func.GetVariableText("var1"));
      Utils.AssertRawQuery(q2, @"SELECT ""id"" FROM model WHERE (""name"") = (current_setting('vars.var1')::text)");
    }

    [Fact]
    public void FuncAggregateMaxMin()
    {
      var q1 = Builders<MyModel>.Select(x => AggregateFunc.Max(x.Id)).Where(x => x.DateTime > Func.Now());
      Utils.AssertRawQuery(q1, @"SELECT MAX(""id"") FROM model WHERE (datetime) > (NOW())");

      var q2 = Builders<MyModel>.Select(x => AggregateFunc.Min(x.Id)).Where(x => x.DateTime > Func.Now());
      Utils.AssertRawQuery(q2, @"SELECT MIN(""id"") FROM model WHERE (datetime) > (NOW())");
    }

    [Fact]
    public void FuncAggregateSum()
    {
      var q1 = Builders<MyModel>.Select(x => AggregateFunc.Sum(x.ValFloat));
      Utils.AssertRawQuery(q1, @"SELECT SUM(val_f32) FROM model");
    }

    [Fact]
    public void FuncDate()
    {
      var dt = new DateTime(2000, 1, 2, 3, 4, 5);
      var q = Builders<MyModel>.Select(x => x.Id).Where(x => Func.Date(x.DateTime) == Func.Date(dt));

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM model WHERE (DATE(datetime)) = (DATE(@1::timestamp))",
                           new[] { new Param(dt, NpgsqlDbType.Timestamp), });
    }

    [Fact]
    public void FuncTimezone()
    {
      var dt = new DateTime(2000, 1, 2, 3, 4, 5);
      var q = Builders<MyModel>.Select(x => x.Id).Where(x => Func.Timezone("UTC", x.DateTime) == dt);

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM model WHERE (timezone('UTC',datetime)) = (@1::timestamp)",
                           new[] { new Param(dt, NpgsqlDbType.Timestamp), });
    }

    [Fact]
    public void FuncExtract()
    {
      // Extract
      var q1 = Builders<MyModel>.Select(x => x.Id).Where(x => Func.Extract(ExtractField.Day, x.DateTime) == 2.0);
      Utils.AssertRawQuery(q1, @"SELECT ""id"" FROM model WHERE (EXTRACT('day' FROM datetime)) = (@1::double precision)",
                           new Param(2.0, NpgsqlDbType.Double));

      var q2 = Builders<MyModel>.Select(x => x.Id).Where(x => (int)Func.Extract(ExtractField.Day, x.DateTime) == 2);
      Utils.AssertRawQuery(q2, @"SELECT ""id"" FROM model WHERE ((EXTRACT('day' FROM datetime))::int) = (2)");
      
      // DatePart
      var q3 = Builders<MyModel>.Select(x => x.Id).Where(x => Func.DatePart(ExtractField.Day, x.DateTime) == 2.0);
      Utils.AssertRawQuery(q3, @"SELECT ""id"" FROM model WHERE (date_part('day',datetime)) = (@1::double precision)",
                           new Param(2.0, NpgsqlDbType.Double));

      var q4 = Builders<MyModel>.Select(x => x.Id).Where(x => (int)Func.DatePart(ExtractField.Day, x.DateTime) == 2);
      Utils.AssertRawQuery(q4, @"SELECT ""id"" FROM model WHERE ((date_part('day',datetime))::int) = (2)");

      // Direct
      var q5 = Builders<MyModel>.Select(x => x.Id).Where(x => x.DateTime.Day == 4);
      Utils.AssertRawQuery(q5, @"SELECT ""id"" FROM model WHERE ((date_part('day',datetime))::int) = (4)");
    }

    [Fact]
    public void FuncInterval()
    {
      var dt = new DateTime(2000, 1, 2, 3, 4, 5);
      var ts = TimeSpan.FromHours(1.23);
      var q = Builders<MyModel>.Select(x => x.Id).Where(x => x.DateTime + ts > dt);

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM model WHERE ((datetime) + (@1::interval)) > (@2::timestamp)",
                           new Param(ts, NpgsqlDbType.Interval),
                           new Param(dt, NpgsqlDbType.Timestamp));
    }
  }
}