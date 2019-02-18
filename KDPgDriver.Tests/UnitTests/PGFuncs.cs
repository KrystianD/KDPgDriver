using System;
using KDPgDriver.Builders;
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

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".model WHERE (MD5(""name"")) = ('hex')");
    }

    [Fact]
    public void FuncCount()
    {
      var q = Builders<MyModel>.Select(x => Func.Count(x.Id));

      Utils.AssertRawQuery(q, @"SELECT COUNT(""id"") FROM ""public"".model");
    }

    [Fact]
    public void FuncNow()
    {
      var q = Builders<MyModel>.Select(x => x.Id).Where(x => x.DateTime > Func.Now());

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".model WHERE (datetime) > (NOW())");
    }

    [Fact]
    public void FuncRaw()
    {
      var q = Builders<MyModel>.Select(x => x.Id).Where(x => x.DateTime > Func.Raw<DateTime>("NOW() + INTERVAL 2 SECONDS"));

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM ""public"".model WHERE (datetime) > (NOW() + INTERVAL 2 SECONDS)");
    }

    [Fact]
    public void FuncGetVariable()
    {
      var q1 = Builders<MyModel>.Select(x => x.Id).Where(x => x.Id > Func.GetVariableInt("var1"));
      Utils.AssertRawQuery(q1, @"SELECT ""id"" FROM ""public"".model WHERE (""id"") > (current_setting('vars.var1')::int)");

      var q2 = Builders<MyModel>.Select(x => x.Id).Where(x => x.Name == Func.GetVariableText("var1"));
      Utils.AssertRawQuery(q2, @"SELECT ""id"" FROM ""public"".model WHERE (""name"") = (current_setting('vars.var1')::text)");
    }

    [Fact]
    public void FuncAggregateMaxMin()
    {
      var q1 = Builders<MyModel>.Select(x => AggregateFunc.Max(x.Id)).Where(x => x.DateTime > Func.Now());
      Utils.AssertRawQuery(q1, @"SELECT MAX(""id"") FROM ""public"".model WHERE (datetime) > (NOW())");
   
      var q2 = Builders<MyModel>.Select(x => AggregateFunc.Min(x.Id)).Where(x => x.DateTime > Func.Now());
      Utils.AssertRawQuery(q2, @"SELECT MIN(""id"") FROM ""public"".model WHERE (datetime) > (NOW())");
    }
  }
}