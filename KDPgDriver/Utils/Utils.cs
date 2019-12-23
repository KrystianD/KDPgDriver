using System;
using System.Collections.Generic;
using System.Data;

namespace KDPgDriver.Utils
{
  internal static class Utils
  {
    public static bool CheckIfEnumerable(Type type, out Type itemType)
    {
      itemType = null;

      foreach (var i in type.GetInterfaces()) {
        var isEnumerable = i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>);
        if (isEnumerable) {
          itemType = i.GetGenericArguments()[0];
          return true;
        }
      }

      return false;
    }
    
    internal static IsolationLevel ToIsolationLevel(KDPgIsolationLevel level)
    {
      switch (level) {
        case KDPgIsolationLevel.ReadCommitted: return IsolationLevel.ReadCommitted;
        case KDPgIsolationLevel.RepeatableRead: return IsolationLevel.RepeatableRead;
        case KDPgIsolationLevel.Serializable: return IsolationLevel.Serializable;
        default: throw new ArgumentOutOfRangeException(nameof(level), level, null);
      }
    }
  }
}