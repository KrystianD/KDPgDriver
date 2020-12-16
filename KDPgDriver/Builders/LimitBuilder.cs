namespace KDPgDriver.Builders
{
  public class LimitBuilder
  {
    internal int? LimitValue { get; set; }
    internal int? OffsetValue { get; set; }

    public LimitBuilder Limit(int value)
    {
      LimitValue = value;
      return this;
    }

    public LimitBuilder Offset(int value)
    {
      OffsetValue = value;
      return this;
    }
  }
}