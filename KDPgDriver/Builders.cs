using KDPgDriver.Builder;

namespace KDPgDriver
{
  public static class Builders<T>
  {
    public static QueryBuilder<T> Query => new QueryBuilder<T>();
    public static InsertQuery<T> Insert => new InsertQuery<T>();
  }
}