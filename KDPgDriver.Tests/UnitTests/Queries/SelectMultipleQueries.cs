﻿using System;
using KDPgDriver.Builders;
using Xunit;

namespace KDPgDriver.Tests.UnitTests.Queries
{
  public class SelectMultipleQueriesUnitTests
  {
    static SelectMultipleQueriesUnitTests()
    {
      MyInit.Init();
    }

    [Fact]
    public void SelectMultiple()
    {
      var q = BuildersJoin.FromMany<MyModel, MyModel2>((a, b) => a.Id == b.ModelId)
                          .Map((a, b) => new {
                              M1 = a,
                              M2 = b,
                          })
                          .Select();

      Utils.AssertRawQuery(q, @"
SELECT 
  t0.id,t0.name,t0.list_string,t0.list_string2,(t0.enum)::text,(t0.list_enum)::text[],(t0.enum2)::text,t0.datetime,t0.json_object1,t0.json_model,t0.json_array1,t1.id,t1.name,t1.model_id
FROM
  public.model t0 LEFT JOIN public.model2 t1 ON ((t0.id) = (t1.model_id))");
    }

    [Fact]
    public void SelectMultipleSub()
    {
      var q = BuildersJoin.FromMany<MyModel, MyModel2>((a, b) => a.Id == b.ModelId)
                          .Map((a, b) => new {
                              M1 = a,
                              M2 = b,
                          })
                          .Select(x => new {
                              M1 = x.M1,
                              M2_name = x.M2.Name,
                              M3_calc = x.M2.Id * 2,
                          });

      Utils.AssertRawQuery(q, @"
SELECT 
  t0.id,t0.name,t0.list_string,t0.list_string2,(t0.enum)::text,(t0.list_enum)::text[],(t0.enum2)::text,t0.datetime,t0.json_object1,t0.json_model,t0.json_array1,t1.name,(t1.id) * (2)
FROM
  public.model t0 LEFT JOIN public.model2 t1 ON ((t0.id) = (t1.model_id))");
    }

    [Fact]
    public void SelectMultipleLeftJoin()
    {
      var q = BuildersJoin.FromMany<MyModel, MyModel2>((a, b) => a.Id == b.ModelId)
                          .Map((a, b) => new {
                              M1 = a,
                              M2 = b,
                          })
                          .Select(x => new {
                              x.M1.Id,
                              x.M2.Name,
                          });
      
      Utils.AssertRawQuery(q, @"SELECT t0.id,t1.name FROM public.model t0 LEFT JOIN public.model2 t1 ON ((t0.id) = (t1.model_id))");
      
      var q2 = BuildersJoin.FromMany<MyModel, MyModel2>((a, b) => a.Id == b.ModelId)
                          .Map((a, b) => new {
                              M1 = b,
                              M2 = a,
                          })
                          .Select(x => new {
                              x.M1.Id,
                              x.M2.Name,
                          });
      
      Utils.AssertRawQuery(q2, @"SELECT t1.id,t0.name FROM public.model t0 LEFT JOIN public.model2 t1 ON ((t0.id) = (t1.model_id))");
    }
  }
}