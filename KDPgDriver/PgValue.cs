namespace KDPgDriver.Utils
{
  public class PgValue
  {
    public object Value { get; }
    public KDPgValueType Type { get; }

    public PgValue(object value, KDPgValueType type)
    {
      Value = value;
      Type = type;
    }

    public static readonly PgValue Null = new PgValue(null, KDPgValueTypeInstances.Null);
  }
}