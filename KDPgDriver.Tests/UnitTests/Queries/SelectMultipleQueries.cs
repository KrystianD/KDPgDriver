using System;
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
      var q = BuildersJoin.FromMany<MyModel, MyModel2>()
                          .Map((a, b) => new {
                              M1 = a,
                              M2 = b,
                          })
                          .Select();

      Utils.AssertRawQuery(q, @"
SELECT 
  t1.id,t1.name,t1.list_string,t1.list_string2,(t1.enum)::text,(t1.list_enum)::text[],(t1.enum2)::text,t1.datetime,t1.json_object1,t1.json_model,t1.json_array1,t2.id,t2.name,t2.model_id
FROM
  public.model t1,public.model2 t2");
    }

    [Fact]
    public void SelectMultipleSub()
    {
      var q = BuildersJoin.FromMany<MyModel, MyModel2>()
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
  t1.id,t1.name,t1.list_string,t1.list_string2,(t1.enum)::text,(t1.list_enum)::text[],(t1.enum2)::text,t1.datetime,t1.json_object1,t1.json_model,t1.json_array1,t2.name,(t2.id) * (2)
FROM
  public.model t1,public.model2 t2");
    }

    [Fact]
    public void SelectMultipleJoin()
    {
      var q = BuildersJoin.FromMany<MyModel, MyModel2>()
                          .Map((a, b) => new {
                              M1 = a,
                              M2 = b,
                          })
                          .Select(model => new {
                              A1 = model.M1.Name,
                              A2 = model.M2.Name,
                          })
                          .Where(x => x.M2.ModelId == x.M1.Id)
                          .Where(x => x.M1.Id == 3);

      Utils.AssertRawQuery(q, @"SELECT t1.name,t2.name FROM public.model t1,public.model2 t2 WHERE ((t2.model_id) = (t1.id)) AND ((t1.id) = (3))");

      var q2 = BuildersJoin.FromMany<MyModel, MyModel2>()
                           .Map((a, b) => new {
                               M1 = b,
                               M2 = a,
                           })
                           .Select(model => new {
                               A1 = model.M1.Name,
                               A2 = model.M2.Name,
                           })
                           .Where(x => x.M1.ModelId == x.M2.Id)
                           .Where(x => x.M2.Id == 3);

      Utils.AssertRawQuery(q2, @"SELECT t2.name,t1.name FROM public.model t1,public.model2 t2 WHERE ((t2.model_id) = (t1.id)) AND ((t1.id) = (3))");
    }
  }
}