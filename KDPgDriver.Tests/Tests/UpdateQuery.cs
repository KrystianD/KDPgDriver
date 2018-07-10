using System;
using System.Collections.Generic;
using KDPgDriver.Builder;
using KDPgDriver.Utils;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests
{
  public class UpdateQuery
  {
    static UpdateQuery()
    {
      MyInit.Init();
    }

    [Fact]
    public void UpdateSetField()
    {
      var q = Builders<MyModel>.Query.Update(
          Builders<MyModel>.UpdateOp.SetField(x => x.Name, "A"));

      Utils.AssertRawQuery(q, @"UPDATE public.model SET name = 'A'");
    }

    [Fact]
    public void UpdateSetFieldDateTime()
    {
      var date = (DateTime?)DateTime.Parse("2018-01-01 12:34");
      
      var q = Builders<MyModel>.Query.Update(
          Builders<MyModel>.UpdateOp.SetField(x => x.DateTime, date));

      Utils.AssertRawQuery(q, @"UPDATE public.model SET datetime = @1::timestamp",
                           new Param(date, NpgsqlDbType.Timestamp));
    }

    [Fact]
    public void UpdateAddToList()
    {
      var q = Builders<MyModel>.Query.Update(
          Builders<MyModel>.UpdateOp.AddToList(x => x.ListString, "A"));

      Utils.AssertRawQuery(q, @"UPDATE public.model SET list_string = array_cat(list_string, @1::text[])",
                           new Param(new List<string>() { "A" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    [Fact]
    public void UpdateRemoveFromList()
    {
      var q = Builders<MyModel>.Query.Update(
          Builders<MyModel>.UpdateOp.RemoveFromList(x => x.ListString, "A"));

      Utils.AssertRawQuery(q, @"UPDATE public.model SET list_string = array_remove(list_string, 'A')");
    }

    [Fact]
    public void UpdateListOperationsCombined()
    {
      var q = Builders<MyModel>.Query.Update(
          Builders<MyModel>.UpdateOp
                           .AddToList(x => x.ListString, "A")
                           .RemoveFromList(x => x.ListString, "B")
                           .AddToList(x => x.ListString2, "C")
      );

      Utils.AssertRawQuery(q, @"UPDATE public.model SET list_string = array_remove(array_cat(list_string, @1::text[]), 'B'), list_string2 = array_cat(list_string2, @2::text[])",
                           new Param(new List<string>() { "A" }, NpgsqlDbType.Array | NpgsqlDbType.Text),
                           new Param(new List<string>() { "C" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }
  }
}