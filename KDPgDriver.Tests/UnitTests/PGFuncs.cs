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
      Utils.AssertRawQuery(new QueryBuilder<MyModel>().Where(x => Func.MD5(x.Name) == "hex"),
                           @"SELECT id FROM public.model WHERE (MD5(name)) = ('hex')");
    }
  }
}