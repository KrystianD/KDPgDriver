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

      Utils.AssertRawQuery(q, @"SELECT id FROM public.model WHERE (MD5(name)) = ('hex')");
    }

    [Fact]
    public void FuncCount()
    {
      var q = Builders<MyModel>.Select(x => Func.Count(x.Id));

      Utils.AssertRawQuery(q, @"SELECT COUNT(id) FROM public.model");
    }

    [Fact]
    public void FuncNow()
    {
      var q = Builders<MyModel>.Select(x => x.Id).Where(x => x.DateTime > Func.Now());

      Utils.AssertRawQuery(q, @"SELECT id FROM public.model WHERE (datetime) > (NOW())");
    }
  }
}