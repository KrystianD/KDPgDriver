using System.Collections.Generic;

namespace KDPgDriver.Tests
{
  public static class Init
  {
  }

  public enum MyEnum
  {
    A,
    B,
    C
  }

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

    [KDPgColumn("enum")]
    public MyEnum Enum { get; set; }

    [KDPgColumn("list_enum")]
    public List<MyEnum> ListEnum { get; set; }
  }
}