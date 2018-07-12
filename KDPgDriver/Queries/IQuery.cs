namespace KDPgDriver.Builders
{
  public interface IQuery
  {
    RawQuery GetRawQuery(string defaultSchema = null);
  }
}