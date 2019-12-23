using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KDPgDriver.Tests
{
  [KDPgTable("model2", schema: "public")]
  public class MyModel2
  {
    [KDPgColumn("id", KDPgColumnFlagsEnum.PrimaryKey)]
    public int Id { get; set; }

    [KDPgColumn("name1")]
    public string Name1 { get; set; }

    [KDPgColumn("model_id")]
    public int? ModelId { get; set; }
  }
}