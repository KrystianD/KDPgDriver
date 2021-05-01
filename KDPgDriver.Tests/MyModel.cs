using System;
using System.Collections.Generic;
using KDPgDriver.Types;
using Newtonsoft.Json;
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

  public class MySubsubmodel
  {
    [JsonProperty("number")]
    public int Number { get; set; }

    [JsonProperty("name")]
    public string Name { get; set; }
  }

  public class MySubmodel
  {
    [JsonProperty("number")]
    public int Number { get; set; }

    [JsonProperty("name")]
    public string Name { get; set; }

    [JsonProperty("decimal")]
    public decimal Decimal { get; set; }

    [JsonProperty("inner")]
    public MySubsubmodel MySubsubmodel { get; set; }

    [JsonProperty("json_object2")]
    public JObject JsonObject2 { get; set; }

    [JsonProperty("json_array2")]
    public JArray JsonArray2 { get; set; }
  }

  [KDPgTable("model")]
  public class MyModel
  {
    [KDPgColumn("id", KDPgColumnFlagsEnum.PrimaryKey | KDPgColumnFlagsEnum.AutoIncrement)]
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

    [KDPgColumn("date")]
    [KDPgColumnType(KDPgValueTypeKind.Date)]
    public DateTime Date { get; set; }

    [KDPgColumn("time")]
    [KDPgColumnType(KDPgValueTypeKind.Time)]
    public TimeSpan Time { get; set; }

    [KDPgColumn("datetime")]
    public DateTime DateTime { get; set; }

    [KDPgColumn("json_object1")]
    public JObject JsonObject1 { get; set; }

    [KDPgColumn("json_model")]
    [KDPgColumnType(KDPgValueTypeKind.Json)]
    public MySubmodel JsonModel { get; set; }

    [KDPgColumn("json_array1")]
    public JArray JsonArray1 { get; set; }

    [KDPgColumn("bool")]
    public bool Bool { get; set; }

    [KDPgColumn("binary")]
    public byte[] Binary { get; set; }

    [KDPgColumn("private_int")]
    private int _privateInt { get; set; }

    public int PrivateInt => _privateInt;

    [KDPgColumn("val_f32")]
    public float ValFloat { get; set; }

    [KDPgColumn("val_f64")]
    public double ValDouble { get; set; }
  }

  [KDPgTable("model_link1")]
  public class MyModelLink1
  {
    [KDPgColumn("id", KDPgColumnFlagsEnum.PrimaryKey | KDPgColumnFlagsEnum.AutoIncrement)]
    public int Id { get; set; }
  }

  [KDPgTable("model_link2")]
  public class MyModelLink2
  {
    [KDPgColumn("id", KDPgColumnFlagsEnum.PrimaryKey | KDPgColumnFlagsEnum.AutoIncrement)]
    public int Id { get; set; }

    [KDPgColumn("link_id")]
    public int LinkID { get; set; }
  }

  [KDPgTable("model_nopk")]
  public class MyModelNoPK
  {
    [KDPgColumn("name")]
    public string Name { get; set; }
  }
}