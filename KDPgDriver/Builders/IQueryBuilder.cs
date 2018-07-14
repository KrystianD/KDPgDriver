namespace KDPgDriver.Builders
{
  public interface IQueryBuilder
  {
    // string TableName { get; }
    // string SchemaName { get; }

    IWhereBuilder GetWhereBuilder();
  }
}