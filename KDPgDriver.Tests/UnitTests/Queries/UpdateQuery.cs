﻿using System;
using System.Collections.Generic;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests.UnitTests.Queries
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
      var date = (DateTime?) DateTime.Parse("2018-01-01 12:34");

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

      Utils.AssertRawQuery(q, @"UPDATE public.model SET list_string = array_cat(list_string, array['A'])");
    }

    [Fact]
    public void UpdateAddToJsonList()
    {
      var q = Builders<MyModel>.Query.Update(
          Builders<MyModel>.UpdateOp.AddToList(x => x.JsonArray1, "A"));

      Utils.AssertRawQuery(q, @"UPDATE public.model SET json_array1 = kdpg_jsonb_add(json_array1, array[], to_jsonb(@1::jsonb)",
                           new Param("\"A\"", NpgsqlDbType.Jsonb));
    }

    [Fact]
    public void UpdateRemoveFromList()
    {
      var q = Builders<MyModel>.Query.Update(
          Builders<MyModel>.UpdateOp.RemoveFromList(x => x.ListString, "A"));

      Utils.AssertRawQuery(q, @"UPDATE public.model SET list_string = array_remove(list_string, 'A')");
    }

    [Fact]
    public void UpdateListOperationsCombined1()
    {
      var q = Builders<MyModel>.Query.Update(
          Builders<MyModel>.UpdateOp
                           .AddToList(x => x.ListString, "A")
                           .RemoveFromList(x => x.ListString, "B")
                           .AddToList(x => x.ListString2, "C")
      );

      Utils.AssertRawQuery(q, @"
UPDATE public.model
SET list_string = array_remove(array_cat(list_string, array['A']), 'B'),
    list_string2 = array_cat(list_string2, array['C'])");
    }

    [Fact]
    public void UpdateListOperationsCombined2()
    {
      var q = Builders<MyModel>.Query.Update(
          Builders<MyModel>.UpdateOp
                           .SetField(x => x.ListString, new List<string>() { "a1", "a2" })
                           .AddToList(x => x.ListString, "A")
                           .RemoveFromList(x => x.ListString, "B")
      );

      Utils.AssertRawQuery(q, @"
UPDATE public.model
SET list_string = array_remove(array_cat(@1::text[], array['A']), 'B')",
                           new Param(new List<string>() { "a1", "a2" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }
  }
}