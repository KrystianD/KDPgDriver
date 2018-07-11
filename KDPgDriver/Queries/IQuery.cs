namespace KDPgDriver.Builder
{
  public interface IQuery
  {
    RawQuery GetRawQuery(string defaultSchema = null);
  }
}