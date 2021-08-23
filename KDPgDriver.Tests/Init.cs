namespace KDPgDriver.Tests
{
  public static class MyInit
  {
    private static T ParseEnum<T>(string name) where T : struct
    {
      MyEnum.TryParse<T>(name, out var res);
      return res;
    }

    public static void Init()
    {
      TypeRegistry.RegisterEnum<MyEnum>("enum", x => x.ToString(), ParseEnum<MyEnum>);
      TypeRegistry.RegisterEnum<MyEnum2>("enum2", x => x.ToString(), ParseEnum<MyEnum2>, schema: "Schema1");
      TypeRegistry.RegisterNativeEnumAsText<MyEnumText>();
    }
  }
}