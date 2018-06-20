using System;
using KDPgDriver.Builder;
using KDPgDriver.Utils;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests
{
  public class SelectQueriesUnitTests
  {
    [Fact]
    public void SelectSimple()
    {
      var builder = new QueryBuilder<MyModel>();
      var q = builder.Select();

      Utils.AssertRawQuery(q, @"SELECT ""id"",""name"",""list_string"",""list_string2"" FROM ""public"".""model""");
    }

    [Fact]
    public void SelectColumns()
    {
      var builder = new QueryBuilder<MyModel>();
      var q = builder.Select(x => new
      {
          x.Name
      });

      Utils.AssertRawQuery(q, @"SELECT ""name"" FROM ""public"".""model""");
    }

    [Fact]
    public void SelectColumnsExpressionString()
    {
      var builder = new QueryBuilder<MyModel>();
      var q = builder.Select(x => new
      {
          OutName = x.Name + "A" + "B"
      });

      Utils.AssertRawQuery(q, @"SELECT ((""name"") || ('A')) || ('B') FROM ""public"".""model""");
    }

    [Fact]
    public void SelectColumnsExpressionNumber()
    {
      var builder = new QueryBuilder<MyModel>();
      var q = builder.Select(x => new
      {
          OutId = (x.Id + 2 * 3 + 4) * 5,
      });

      Utils.AssertRawQuery(q, @"SELECT (((""id"") + (6)) + (4)) * (5) FROM ""public"".""model""");
    }

    [Fact]
    public void SelectColumnsExpressionFunc()
    {
      var builder = new QueryBuilder<MyModel>();
      var q = builder.Select(x => new
      {
          OutName = x.Name.Substring(5, 10),
      });

      Utils.AssertRawQuery(q.GetQuery(null), @"SELECT substring((""name"") from (5) for (10)) FROM ""public"".""model""");
    }
  }
}