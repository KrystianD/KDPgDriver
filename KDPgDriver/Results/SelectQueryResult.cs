using System;
using System.Collections.Generic;

namespace KDPgDriver.Results
{
  public class SelectQueryResult<T>
  {
    private readonly List<T> _objects;

    internal SelectQueryResult(List<T> objects)
    {
      _objects = objects;
    }

    public List<T> GetAll() => _objects;

    public T GetSingle() => _objects.Count == 0 ? throw new Exception("no results found") : _objects[0];

    public T GetSingleOrDefault(T def = default) => _objects.Count == 0 ? def : _objects[0];
  }
}