using Xunit;

namespace KDPgDriver.Tests.UnitTests.Queries
{
  public class DeleteQuery
  {
    static DeleteQuery()
    {
      MyInit.Init();
    }

    [Fact]
    public void DeleteSimple()
    {
      var q = Builders<MyModel>.Delete();

      Utils.AssertRawQuery(q, @"DELETE FROM model");
    }

    [Fact]
    public void DeleteWhere()
    {
      var q = Builders<MyModel>.Delete().Where(x => x.Id == 2);

      Utils.AssertRawQuery(q, @"DELETE FROM model WHERE (""id"") = (2)");
    }
  }
}