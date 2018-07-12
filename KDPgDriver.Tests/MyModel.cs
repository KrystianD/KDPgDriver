using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace KDPgDriver.Tests
{
  public static class Init { }

  public enum MyEnum
  {
    A,
    B,
    C
  }

  public enum MyEnum2
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

    [KDPgColumn("enum2")]
    public MyEnum2 Enum2 { get; set; }

    [KDPgColumn("datetime")]
    public DateTime DateTime { get; set; }
  }
}