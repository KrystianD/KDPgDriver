using KDPgDriver.Traverser;
using KDPgDriver.Utils;
using Newtonsoft.Json.Linq;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests.UnitTests
{
  public class JsonTypesTests
  {
    static JsonTypesTests()
    {
      MyInit.Init();
    }

    // JSON
    [Fact]
    public void ComparisonJson()
    {
      TypedExpression exp;

      exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.JsonObject1["A"] == JToken.FromObject("A"));

      Utils.AssertExpression(exp, @"((json_object1)->'A') = to_jsonb((@1::jsonb))",
                             new Param("\"A\"", NpgsqlDbType.Jsonb));

      exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.JsonObject1["A"] == JToken.FromObject(2));

      Utils.AssertExpression(exp, @"((json_object1)->'A') = to_jsonb((@1::jsonb))",
                             new Param("2", NpgsqlDbType.Jsonb));
    }

    [Fact]
    public void TypeNumber()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.JsonModel.Number == 2);
      Utils.AssertExpression(exp, @"((json_model->>'number')::int) = (2)");
    }

    [Fact]
    public void TypeString()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.JsonModel.Name == "A");
      Utils.AssertExpression(exp, @"(json_model->>'name') = ('A')");
    }

    [Fact]
    public void TypeDecimal()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.JsonModel.Decimal == 1.234M);
      Utils.AssertExpression(exp, @"((json_model->>'decimal')::numeric) = (@1::numeric)",
                             new Param(1.234M, NpgsqlDbType.Numeric));
    }
  }
}