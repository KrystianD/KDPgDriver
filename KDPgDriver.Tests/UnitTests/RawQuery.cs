using System.Linq.Expressions;
using KDPgDriver.Builders;
using KDPgDriver.Traverser;
using KDPgDriver.Types;
using KDPgDriver.Utils;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests.UnitTests
{
  public class Param
  {
    public object Value { get; }
    public NpgsqlDbType Type { get; }

    public Param(object value, NpgsqlDbType type)
    {
      Value = value;
      Type = type;
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

    [Fact]
    public void WithColumn()
    {
      var t1 = ModelsRegistry.GetTable<MyModel>();
      var t2 = ModelsRegistry.GetTable<MyModel2>();

      var rq = new RawQuery();
      rq.AppendColumn(NodeVisitor.EvaluateFuncExpressionToColumn<MyModel>(x => x.Name), new RawQuery.TableNamePlaceholder(t1, "M1"));
      rq.Append(" = 123, ");

      rq.AppendColumn(NodeVisitor.EvaluateFuncExpressionToColumn<MyModel2>(x => x.Name1), new RawQuery.TableNamePlaceholder(t2, "M2"));
      rq.Append(" = 456");

      rq.ApplyAlias("M1", "t1");
      rq.ApplyAlias("M2", "t2");

      Utils.AssertRawQueryWithAliases(rq, @"t1.""name"" = 123, t2.name1 = 456");
    }

    [Fact]
    public void WithColumnCombined()
    {
      var t1 = ModelsRegistry.GetTable<MyModel>();
      var t2 = ModelsRegistry.GetTable<MyModel2>();

      var rq1 = new RawQuery();
      rq1.AppendColumn(NodeVisitor.EvaluateFuncExpressionToColumn<MyModel>(x => x.Name), new RawQuery.TableNamePlaceholder(t1, "M1"));
      rq1.Append(" = 123");
      rq1.ApplyAlias("M1", "t1");

      var rq2 = new RawQuery();
      rq2.AppendColumn(NodeVisitor.EvaluateFuncExpressionToColumn<MyModel2>(x => x.Name1), new RawQuery.TableNamePlaceholder(t2, "M2"));
      rq2.Append(" = 456");
      rq2.ApplyAlias("M2", "t2");

      var rq = new RawQuery();
      rq.Append(rq1);
      rq.Append(", ");
      rq.Append(rq2);

      Utils.AssertRawQueryWithAliases(rq, @"t1.""name"" = 123, t2.name1 = 456");
    }
  }
}