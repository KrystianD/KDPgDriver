using KDPgDriver.Traverser;
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
    public void ExpressionJsonString()
    {
      var val = JToken.FromObject("A");

      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.JsonObject1["A"] == val);

      Utils.AssertExpression(exp, @"((json_object1)->'A') = to_jsonb((@1::jsonb))",
                             new Param("\"A\"", NpgsqlDbType.Jsonb));
    }

    [Fact]
    public void ExpressionJsonNumber()
    {
      var val = JToken.FromObject(2);

      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.JsonObject1["A"] == val);

      Utils.AssertExpression(exp, @"((json_object1)->'A') = to_jsonb((@1::jsonb))",
                             new Param("2", NpgsqlDbType.Jsonb));
    }

    [Fact]
    public void ExpressionJsonModel()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.JsonModel.Name);

      Utils.AssertExpression(exp, @"(json_model->'name')::text");
    }
  }
}