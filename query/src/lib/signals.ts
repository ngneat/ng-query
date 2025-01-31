import { Signal, computed } from '@angular/core';
import {
  QueryObserverBaseResult,
  QueryObserverResult,
} from '@tanstack/query-core';

type DataTypes<
  T extends
    | Array<Signal<QueryObserverBaseResult>>
    | Record<string, Signal<QueryObserverBaseResult<any>>>,
> = {
  [P in keyof T]: T[P] extends Signal<QueryObserverBaseResult<infer R>>
    ? R
    : never;
};

type UnifiedTypes<T> = T extends Array<Signal<QueryObserverBaseResult<any>>>
  ? DataTypes<T>
  : T extends Record<string, Signal<QueryObserverBaseResult<any>>>
    ? DataTypes<T>
    : never;

/**
 *
 *  @experimental
 *
 *  This function is used to merge multiple signal queries into one.
 *  It will return a new base query result that will merge the results of all the queries.
 *  Note that it should be used inside injection context.
 *  If you pass the 'intersectStaleData' flag, it will also intersect unsuccessful result in case data for all queries is present.
 *
 * @example
 *
 * const query = intersetResults({
 *   todos: todos.result$,
 *   posts: posts.result$,
 * }, ({ todos, posts }) => {
 *   return todos + posts;
 * });
 *
 * @example
 *
 * const query = intersectResults(
 *   [
 *     this.todosService.getTodo('1').result,
 *     this.todosService.getTodo('2').result,
 *   ],
 *  ([todoOne, todoTwo]) => {
 *    return todoOne.title + todoTwo.title;
 *  }
 * );
 *
 * @example
 *
 * const query = intersetResults({
 *   todos: todos.result$,
 *   posts: posts.result$,
 * }, ({ todos, posts }) => {
 *   return todos + posts;
 * }, { intersectStaleData: true });
 */
export function intersectResults<
  T extends
    | Array<Signal<QueryObserverResult<any>>>
    | Record<string, Signal<QueryObserverResult<any>>>,
  R,
>(
  signals: T,
  mapFn: (values: UnifiedTypes<T>) => R,
  options?: { intersectStaleData: boolean }
): Signal<QueryObserverResult<R> & { all: T }> {
  const isArray = Array.isArray(signals);
  const toArray = isArray ? signals : Object.values(signals);
  const refetch = () => Promise.all(toArray.map(v => v().refetch()));
  const intersectData = isArray
    ? () => toArray.map((r) => r().data) as UnifiedTypes<T>
    : () => Object.entries(signals).reduce((acc, [key, value]) => {
        acc[key as keyof UnifiedTypes<T>] = value().data;
        return acc;
      }, {} as UnifiedTypes<T>);

  return computed(() => {
    const mappedResult = {
      all: signals,
      isSuccess: toArray.every((v) => v().isSuccess),
      isPending: toArray.some((v) => v().isPending),
      isLoading: toArray.some((v) => v().isLoading),
      isError: toArray.some((v) => v().isError),
      isFetching: toArray.some((v) => v().isFetching),
      error: toArray.find((v) => v().isError)?.error,
      data: undefined,
      refetch,
    } as unknown as QueryObserverResult<R> & { all: T };

    if (mappedResult.isSuccess || (options?.intersectStaleData && toArray.every((v) => !!v().data))) {
      mappedResult.data = mapFn(intersectData());
    }

    return mappedResult;
  });
}
