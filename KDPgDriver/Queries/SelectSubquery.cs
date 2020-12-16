using KDPgDriver.Utils;

namespace KDPgDriver.Queries
{
  public interface ISelectSubquery : IQuery
  {
    TypedExpression GetTypedExpression();
  }

  public class SelectSubquery<TValue> : ISelectSubquery
  {
    private readonly ISelectQuery _selectQuery;

    public SelectSubquery(ISelectQuery selectQuery)
    {
      _selectQuery = selectQuery;
    }

    public RawQuery GetRawQuery()
    {
      return _selectQuery.GetRawQuery();
    }

    public TypedExpression GetTypedExpression()
    {
      var itemType = PgTypesConverter.CreatePgValueTypeFromObjectType(typeof(TValue));
      var arrayType = new KDPgValueTypeArray(itemType, typeof(TValue));
      return new TypedExpression(GetRawQuery(), arrayType);
    }
  }
}