namespace KDPgDriver.Results
{
  public class InsertQueryResult
  {
    public bool RowInserted { get; private set; }
    public int? LastInsertId { get; private set; }

    public static InsertQueryResult CreateRowInserted(int lastInsertId)
    {
      return new InsertQueryResult() { RowInserted = true, LastInsertId = lastInsertId, };
    }

    public static InsertQueryResult CreateRowNotInserted()
    {
      return new InsertQueryResult() { RowInserted = false };
    }
  }
}