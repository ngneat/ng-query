import { inject, Injectable, InjectionToken, Signal } from '@angular/core';
import { injectQueryClient } from './query-client';
import {
  DefaultError,
  MutateOptions,
  MutationObserver,
  MutationObserverOptions,
  MutationObserverResult,
  notifyManager,
} from '@tanstack/query-core';
import { isObservable, Observable, shareReplay } from 'rxjs';
import { toSignal } from '@angular/core/rxjs-interop';
import { shouldThrowError, toPromise } from './utils';

export type CreateMutationOptions<
  TData = unknown,
  TError = DefaultError,
  TVariables = void,
  TContext = unknown,
> = Omit<
  MutationObserverOptions<TData, TError, TVariables, TContext>,
  'mutationFn'
> & {
  mutationFn: (variables: TVariables) => Promise<TData> | Observable<TData>;
};

export type MutationResult<
  TData = unknown,
  TError = DefaultError,
  TVariables = void,
  TContext = unknown,
> = {
  mutateAsync: (
    variables: TVariables,
    options?: MutateOptions<TData, TError, TVariables, TContext>,
  ) => Promise<TData>;
  mutate: (
    variables: TVariables,
    options?: MutateOptions<TData, TError, TVariables, TContext>,
  ) => void;
  reset: MutationObserver<TData, TError, TVariables, TContext>['reset'];
  setOptions: MutationObserver<
    TData,
    TError,
    TVariables,
    TContext
  >['setOptions'];
  result$: Observable<
    MutationObserverResult<TData, TError, TVariables, TContext>
  >;
  result: Signal<MutationObserverResult<TData, TError, TVariables, TContext>>;
};

@Injectable({ providedIn: 'root' })
class Mutation {
  #instance = injectQueryClient();

  use<
    TData = unknown,
    TError = DefaultError,
    TVariables = unknown,
    TContext = unknown,
  >(
    options: CreateMutationOptions<TData, TError, TVariables, TContext>,
  ): MutationResult<TData, TError, TVariables, TContext> {
    const mutationObserver = new MutationObserver<
      TData,
      TError,
      TVariables,
      TContext
    >(this.#instance, {
      ...options,
      mutationFn: (variables: TVariables): Promise<TData> => {
        const source: Promise<TData> | Observable<TData> =
          options.mutationFn(variables);

        if (isObservable(source)) return toPromise({ source });

        return source;
      },
    });

    const result$ = new Observable<
      MutationObserverResult<TData, TError, TVariables, TContext>
    >((observer) => {
      observer.next(mutationObserver.getCurrentResult());

      const disposeSubscription = mutationObserver.subscribe(
        notifyManager.batchCalls(
          (
            result: MutationObserverResult<TData, TError, TVariables, TContext>,
          ) => {
            if (
              result.isError &&
              shouldThrowError(mutationObserver!.options.throwOnError, [
                result.error,
              ])
            ) {
              observer.error(result.error);
            } else {
              observer.next(result);
            }
          },
        ),
      );

      return () => disposeSubscription();
    }).pipe(
      shareReplay({
        bufferSize: 1,
        refCount: true,
      }),
    );

    const mutate = (
      variables: TVariables,
      options?: MutateOptions<TData, TError, TVariables, TContext>,
    ) => {
      mutationObserver.mutate(variables, options).catch(() => {
        // noop
      });
    };

    let cachedSignal: undefined | Signal<any>;

    return {
      mutate,
      mutateAsync: mutationObserver.mutate.bind(mutationObserver),
      reset: mutationObserver.reset.bind(mutationObserver),
      setOptions: mutationObserver.setOptions.bind(mutationObserver),
      result$,
      // @experimental signal support
      get result() {
        if (!cachedSignal) {
          cachedSignal = toSignal(this.result$, { requireSync: true });
        }

        return cachedSignal;
      },
    };
  }
}

const UseMutation = new InjectionToken('UseMutation', {
  providedIn: 'root',
  factory() {
    const mutation = new Mutation();
    return mutation.use.bind(mutation);
  },
});

export function injectMutation() {
  return inject(UseMutation);
}
