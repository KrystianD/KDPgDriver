using System;
using KDPgDriver.Builder;
using KDPgDriver.Utils;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests
{
  public class Param
  {
    public object value;
    public NpgsqlDbType type;

    public Param(object value, NpgsqlDbType type)
    {
      this.value = value;
      this.type = type;
    }
  }

  public class RawQueryUnitTests
  {
    static RawQueryUnitTests()
    {
      MyInit.Init();
    }

    [Fact]
    public void Simple()
    {
      var rq = new RawQuery();
      rq.Append("id = 123");

      Utils.AssertRawQuery(rq, "id = 123");
    }

    [Fact]
    public void Nested()
    {
      var rq1 = new RawQuery();
      rq1.Append("id = 12");

      var rq2 = new RawQuery();
      rq2.Append("id = 34");

      var rq3 = new RawQuery();
      rq3.Append(rq1);
      rq3.Append(" OR ");
      rq3.Append(rq2);

      Utils.AssertRawQuery(rq1, "id = 12");
      Utils.AssertRawQuery(rq2, "id = 34");

      Utils.AssertRawQuery(rq3, "id = 12 OR id = 34");
    }

    [Fact]
    public void NestedSurrounded()
    {
      var rq1 = new RawQuery();
      rq1.Append("id = 12");

      var rq2 = new RawQuery();
      rq2.Append("id = 34");

      var rq3 = new RawQuery();
      rq3.AppendSurround(rq1);
      rq3.Append(" OR ");
      rq3.AppendSurround(rq2);

      Utils.AssertRawQuery(rq1, "id = 12");
      Utils.AssertRawQuery(rq2, "id = 34");

      Utils.AssertRawQuery(rq3, "(id = 12) OR (id = 34)");
    }
  }
}