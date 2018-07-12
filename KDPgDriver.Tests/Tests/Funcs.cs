using System;
using KDPgDriver.Builder;
using KDPgDriver.Utils;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests
{
  public class Funcs
  {
    static Funcs()
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