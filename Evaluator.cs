using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace KDPgDriver {
  public static class Evaluator
  {
    public static Expression PartialEval(Expression expression)
    {
      return new SubtreeEvaluator(new Nominator().Nominate(expression)).Eval(expression);
    }

    /// <summary>
    /// Evaluates & replaces sub-trees when first candidate is reached (top-down)
    /// </summary>
    class SubtreeEvaluator : ExpressionVisitor
    {
      HashSet<Expression> candidates;

      internal SubtreeEvaluator(HashSet<Expression> candidates)
      {
        this.candidates = candidates;
      }

      internal Expression Eval(Expression exp)
      {
        return Visit(exp);
      }

      public override Expression Visit(Expression exp)
      {
        if (exp == null)
          return null;
        if (candidates.Contains(exp))
          return Evaluate(exp);
        return base.Visit(exp);
      }

      private Expression Evaluate(Expression e)
      {
        if (e.NodeType == ExpressionType.Constant)
          return e;
        LambdaExpression lambda = Expression.Lambda(e);
        Delegate fn = lambda.Compile();
        return Expression.Constant(fn.DynamicInvoke(null), e.Type);
      }
    }

    /// <summary>
    /// Performs bottom-up analysis to determine which nodes can possibly
    /// be part of an evaluated sub-tree.
    /// </summary>
    class Nominator : ExpressionVisitor
    {
      HashSet<Expression> candidates;
      bool cannotBeEvaluated;

      internal HashSet<Expression> Nominate(Expression expression)
      {
        candidates = new HashSet<Expression>();
        Visit(expression);
        return candidates;
      }

      private static bool CanBeEvaluatedLocally(Expression expression)
      {
        return expression.NodeType != ExpressionType.Parameter;
      }

      public override Expression Visit(Expression expression)
      {
        if (expression == null)
          return null;

        bool saveCannotBeEvaluated = cannotBeEvaluated;
        cannotBeEvaluated = false;
        base.Visit(expression);
        if (!cannotBeEvaluated) {
          if (CanBeEvaluatedLocally(expression)) {
            candidates.Add(expression);
          }
          else {
            cannotBeEvaluated = true;
          }
        }

        cannotBeEvaluated |= saveCannotBeEvaluated;
        return expression;
      }
    }
  }
}