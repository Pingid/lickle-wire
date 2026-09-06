export type Validate<Input = unknown, Output = Input> =
  | ((payload: Input) => Output | Validate.Result<Output> | undefined)
  | Validate.StandardSchemaV1<Input, Output>

export type InferOutput<S> = Validate.InferOutput<S>
export type StandardResult<Output> = Validate.Result<Output>
export type StandardSchemaV1<Input = unknown, Output = Input> = Validate.StandardSchemaV1<Input, Output>

export declare namespace Validate {
  /**
   * The Standard Schema v1 interface, vendored so this package takes no runtime
   * dependency on a validation library. Anything implementing it — Zod, Valibot,
   * ArkType, or a hand-rolled object — can be passed wherever a validator is.
   *
   * @see https://standardschema.dev
   */
  export interface StandardSchemaV1<Input = unknown, Output = Input> {
    readonly '~standard': {
      readonly version: 1
      readonly vendor: string
      readonly validate: (value: unknown) => Validate.Result<Output> | Promise<Validate.Result<Output>>
      readonly types?: { readonly input: Input; readonly output: Output } | undefined
    }
  }

  export type Result<Output> =
    | { readonly value: Output; readonly issues?: undefined }
    | { readonly issues: ReadonlyArray<{ readonly message: string }> }

  /** The failure half of a {@link Validate.Result}, and the marker that one is in play. */
  type Issues = { readonly issues: ReadonlyArray<{ readonly message: string }> }

  /**
   * The type a validator narrows to: a schema's output, or a predicate's return
   * with the `undefined` rejection signal and any {@link Validate.Result}
   * wrapper stripped off.
   *
   * A predicate is read as returning a `Result` only when its type admits the
   * failure half, which is the same test `validateSync` makes at runtime.
   */
  export type InferOutput<S> =
    S extends StandardSchemaV1<any, infer O>
      ? O
      : S extends (payload: any) => infer O
        ? [Extract<O, Issues>] extends [never]
          ? Exclude<O, undefined>
          : O extends { readonly value: infer V }
            ? V
            : never
        : never
}

/**
 * Runs a schema synchronously. Decoding happens inside the transport's `next`
 * callback, so there is nowhere to await — a schema that validates
 * asynchronously is a configuration error rather than a bad message.
 */
export const validateSync = (
  schema: Validate<unknown, unknown>,
  value: unknown,
): { ok: true; value: unknown } | { ok: false; message: string } => {
  if (typeof schema === 'function') {
    const result = schema(value)
    if (result === undefined) return { ok: false, message: 'rejected' }
    if (typeof result === 'object' && result !== null && 'issues' in result) {
      const r = result as Validate.Result<unknown>
      return r.issues ? { ok: false, message: r.issues.map((i) => i.message).join('; ') } : { ok: true, value: r.value }
    }
    return { ok: true, value: result }
  }
  const result = schema['~standard'].validate(value)
  if (result instanceof Promise)
    throw new TypeError('Asynchronous schemas are not supported at the decode boundary; use a synchronous validator.')
  return result.issues
    ? { ok: false, message: result.issues.map((i) => i.message).join('; ') }
    : { ok: true, value: result.value }
}
