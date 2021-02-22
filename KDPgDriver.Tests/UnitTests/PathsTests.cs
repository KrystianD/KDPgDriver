using KDPgDriver.Traverser;
using KDPgDriver.Types;
using KDPgDriver.Utils;
using Xunit;

namespace KDPgDriver.Tests.UnitTests
{
  public class PathTests
  {
    static PathTests()
    {
      MyInit.Init();
    }

    // ReSharper disable ParameterOnlyUsedForPreconditionCheck.Local
    private static void AssertPath<TModel>(PathInfo pi, string columnName, string query, KDPgValueType type)
    {
      Assert.Equal(type, pi.Expression.Type);
      Assert.Equal(query, pi.Expression.RawQuery.ToString());
      Assert.Equal(ModelsRegistry.GetTable<TModel>().Columns.Find(x => x.Name == columnName), pi.Column);
    }
    // ReSharper restore ParameterOnlyUsedForPreconditionCheck.Local

    [Fact]
    public void PathTest1()
    {
      var pi = NodeVisitor.VisitPath<MyModel>(x => x.Name);

      AssertPath<MyModel>(pi, "name", "\"name\"", KDPgValueTypeInstances.String);
    }

    [Fact]
    public void PathTest2()
    {
      var pi = NodeVisitor.VisitPath<MyModel>(x => x.JsonModel.Name);

      AssertPath<MyModel>(pi, "json_model", "json_model->>'name'", KDPgValueTypeInstances.String);
    }

    [Fact]
    public void PathTest3()
    {
      var pi = NodeVisitor.VisitPath<MyModel>(x => x.JsonModel.MySubsubmodel.Number);

      AssertPath<MyModel>(pi, "json_model", "(json_model->'inner'->>'number')::int", KDPgValueTypeInstances.Integer);
    }

    [Fact]
    public void PathTest4()
    {
      var pi = NodeVisitor.VisitPath<MyModel>(x => x.JsonObject1["a"][0]);

      AssertPath<MyModel>(pi, "json_object1", "json_object1->'a'->0", KDPgValueTypeInstances.Json);
    }

    [Fact]
    public void PathTest5()
    {
      var pi = NodeVisitor.VisitPath<MyModel>(x => x.JsonArray1[0]);

      AssertPath<MyModel>(pi, "json_array1", "json_array1->0", KDPgValueTypeInstances.Json);
    }

    [Fact]
    public void PathTest6()
    {
      var pi = NodeVisitor.VisitPath<MyModel>(x => x.JsonModel.JsonObject2["a"]);

      AssertPath<MyModel>(pi, "json_model", "json_model->'json_object2'->'a'", KDPgValueTypeInstances.Json);
    }
  }
}