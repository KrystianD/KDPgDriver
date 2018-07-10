using System;
using System.Collections.Generic;
using KDPgDriver.Builder;
using KDPgDriver.Utils;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests
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
      var q = Builders<MyModel>.Query.Delete();

      Utils.AssertRawQuery(q, @"DELETE FROM public.model");
    }

    [Fact]
    public void DeleteWhere()
    {
      var q = Builders<MyModel>.Query.Where(x => x.Id == 2).Delete();

      Utils.AssertRawQuery(q, @"DELETE FROM public.model WHERE ((id) = (2))");
    }
  }
}