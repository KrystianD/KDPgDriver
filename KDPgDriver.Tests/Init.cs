namespace KDPgDriver.Tests
{
  public static class MyInit
  {
    public static void Init()
    {
      TypeRegistry.RegisterEnum<MyEnum>("enum", x => x.ToString(), x =>
      {
        MyEnum.TryParse<MyEnum>(x, out var res);
        return res;
      });

      TypeRegistry.RegisterEnum<MyEnum2>("enum2", x => x.ToString(), x =>
                                         {
                                           MyEnum2.TryParse<MyEnum2>(x, out var res);
                                           return res;
                                         },
                                         schema: "Schema1");
    }
  }
}