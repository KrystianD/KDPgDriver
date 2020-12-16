using KDPgDriver.Utils;

namespace KDPgDriver.Queries
{
  public interface IQuery
  {
    RawQuery GetRawQuery();
  }
}