using System;

namespace KDPgDriver
{
  public class KDPgTableAttribute : Attribute
  {
    public string Name { get; }

    public KDPgTableAttribute(string name)
    {
      Name = name;
    }
  }

  public class KDPgColumnAttribute : Attribute
  {
    public string Name { get; }

    public KDPgColumnAttribute(string name)
    {
      Name = name;
    }
  }
}