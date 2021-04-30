namespace KDPgDriver.Results
{
  public class InsertQueryResult
  {
    public readonly bool RowInserted;
    public readonly int? LastInsertId;

    internal InsertQueryResult(bool rowInserted, int? lastInsertId)
    {
      RowInserted = rowInserted;
      LastInsertId = lastInsertId;
    }
  }
}