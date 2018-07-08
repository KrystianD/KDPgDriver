using KDPgDriver.Builder;

namespace KDPgDriver
{
  public static class Builders<T>
  {
    public static QueryBuilder<T> Query => new QueryBuilder<T>();
    public static InsertQuery<T> Insert => new InsertQuery<T>();
    public static UpdateStatementsBuilder<T> UpdateOp => new UpdateStatementsBuilder<T>();
  }
}