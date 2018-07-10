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
    }
  }
}