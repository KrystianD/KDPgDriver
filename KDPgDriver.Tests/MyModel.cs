using System.Collections.Generic;

namespace KDPgDriver.Tests
{
  [KDPgTable("model", schema: "public")]
  public class MyModel
  {
    [KDPgColumn("id", KDPgColumnFlagsEnum.PrimaryKey)]
    public int Id { get; set; }

    [KDPgColumn("name")]
    public string Name { get; set; }

    [KDPgColumn("list_string")]
    public List<string> ListString { get; set; }

    [KDPgColumn("list_string2")]
    public List<string> ListString2 { get; set; }
  }
}