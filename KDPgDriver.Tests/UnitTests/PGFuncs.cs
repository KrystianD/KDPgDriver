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

      Utils.AssertRawQuery(q,
                           @"SELECT id FROM public.model WHERE (MD5(name)) = ('hex')");
    }
  }
}