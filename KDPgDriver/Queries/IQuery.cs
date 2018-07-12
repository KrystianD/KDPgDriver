using KDPgDriver.Builders;
using KDPgDriver.Utils;

namespace KDPgDriver.Queries
{
  public interface IQuery
  {
    RawQuery GetRawQuery(string defaultSchema = null);
  }
}