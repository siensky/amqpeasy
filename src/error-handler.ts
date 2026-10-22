import {
  BaseError,
  ContentTooLargeError,
  NoRouteError,
  NoConsumersError,
  ConnectionForcedError,
  InvalidPathError,
  AccessRefusedError,
  NotFoundError,
  ResourceLockedError,
  PreconditionFailedError,
  FrameError,
  AmqpSyntaxError,
  CommandInvalidError,
  ChannelError,
  UnexpectedFrameError,
  ResourceError,
  NotAllowedError,
  NotImplementedError,
  InternalError,
} from "./error.js";
import type { OnError } from "./types.js";

const errorClasses: Record<number, new (message?: string, cause?: unknown) => BaseError> = {
  311: ContentTooLargeError,
  312: NoRouteError,
  313: NoConsumersError,
  320: ConnectionForcedError,
  402: InvalidPathError,
  403: AccessRefusedError,
  404: NotFoundError,
  405: ResourceLockedError,
  406: PreconditionFailedError,
  501: FrameError,
  502: AmqpSyntaxError,
  503: CommandInvalidError,
  504: ChannelError,
  505: UnexpectedFrameError,
  506: ResourceError,
  530: NotAllowedError,
  540: NotImplementedError,
  541: InternalError,
};

/** Creates an error reporter scoped to one client. */
export function createErrorReporter(onError?: OnError) {
  const convertedErrors = new WeakMap<object, BaseError>();
  const reportedErrors = new WeakSet<BaseError>();

  function toBaseError(cause: unknown): BaseError {
    if (cause instanceof BaseError) return cause;
    if (typeof cause === "object" && cause !== null) {
      const previous = convertedErrors.get(cause);
      if (previous) return previous;
    }

    const rawCode = typeof cause === "object" && cause !== null && "code" in cause
      ? cause.code
      : undefined;
    const code = typeof rawCode === "number" || typeof rawCode === "string"
      ? rawCode
      : undefined;
    const message = cause instanceof Error ? cause.message : String(cause);
    const ErrorClass = typeof code === "number" ? errorClasses[code] : undefined;
    const error = ErrorClass
      ? new ErrorClass(message, cause)
      : new BaseError(message, code, cause);

    if (typeof cause === "object" && cause !== null) convertedErrors.set(cause, error);
    return error;
  }

  /** Reports each error once and keeps the original error as its cause. */
  function reportError(cause: unknown, background = false): BaseError {
    const error = toBaseError(cause);
    if (reportedErrors.has(error)) return error;
    reportedErrors.add(error);

    if (onError) {
      try {
        Promise.resolve(onError(error)).catch((handlerError: unknown) => {
          console.error("amqpEasy onError failed:", handlerError);
        });
      } catch (handlerError) {
        console.error("amqpEasy onError failed:", handlerError);
      }
    } else if (background) {
      console.error(error);
    }

    return error;
  }

  return reportError;
}
