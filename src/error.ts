/** The base class for all amqpEasy errors. */
export class BaseError extends Error {
  /** The AMQP or system error code, when available. */
  readonly code: number | string | undefined;

  constructor(message: string, code?: number | string, cause?: unknown) {
    super(message, { cause });
    this.name = new.target.name;
    this.code = code;
  }
}

/** 311: The message is too large. */
export class ContentTooLargeError extends BaseError {
  constructor(message = "The message is too large.", cause?: unknown) {
    super(message, 311, cause);
  }
}

/** 312: The message could not be routed to a queue. */
export class NoRouteError extends BaseError {
  constructor(message = "The message could not be routed to a queue.", cause?: unknown) {
    super(message, 312, cause);
  }
}

/** 313: No consumer is available for the message. */
export class NoConsumersError extends BaseError {
  constructor(message = "No consumer is available for the message.", cause?: unknown) {
    super(message, 313, cause);
  }
}

/** 320: RabbitMQ closed the connection. */
export class ConnectionForcedError extends BaseError {
  constructor(message = "RabbitMQ closed the connection.", cause?: unknown) {
    super(message, 320, cause);
  }
}

/** 402: The virtual host path is invalid. */
export class InvalidPathError extends BaseError {
  constructor(message = "The virtual host path is invalid.", cause?: unknown) {
    super(message, 402, cause);
  }
}

/** 403: RabbitMQ refused access. */
export class AccessRefusedError extends BaseError {
  constructor(message = "RabbitMQ refused access.", cause?: unknown) {
    super(message, 403, cause);
  }
}

/** 404: The requested queue or exchange does not exist. */
export class NotFoundError extends BaseError {
  constructor(message = "The requested queue or exchange does not exist.", cause?: unknown) {
    super(message, 404, cause);
  }
}

/** 405: The resource is locked by another connection. */
export class ResourceLockedError extends BaseError {
  constructor(message = "The resource is locked by another connection.", cause?: unknown) {
    super(message, 405, cause);
  }
}

/** 406: The request conflicts with the resource settings. */
export class PreconditionFailedError extends BaseError {
  constructor(message = "The request conflicts with the resource settings.", cause?: unknown) {
    super(message, 406, cause);
  }
}

/** 501: RabbitMQ received an invalid frame. */
export class FrameError extends BaseError {
  constructor(message = "RabbitMQ received an invalid frame.", cause?: unknown) {
    super(message, 501, cause);
  }
}

/** 502: An AMQP command has invalid syntax. */
export class AmqpSyntaxError extends BaseError {
  constructor(message = "An AMQP command has invalid syntax.", cause?: unknown) {
    super(message, 502, cause);
  }
}

/** 503: The AMQP command is invalid. */
export class CommandInvalidError extends BaseError {
  constructor(message = "The AMQP command is invalid.", cause?: unknown) {
    super(message, 503, cause);
  }
}

/** 504: The channel cannot be used for this request. */
export class ChannelError extends BaseError {
  constructor(message = "The channel cannot be used for this request.", cause?: unknown) {
    super(message, 504, cause);
  }
}

/** 505: RabbitMQ received a frame in the wrong order. */
export class UnexpectedFrameError extends BaseError {
  constructor(message = "RabbitMQ received a frame in the wrong order.", cause?: unknown) {
    super(message, 505, cause);
  }
}

/** 506: RabbitMQ does not have enough resources. */
export class ResourceError extends BaseError {
  constructor(message = "RabbitMQ does not have enough resources.", cause?: unknown) {
    super(message, 506, cause);
  }
}

/** 530: RabbitMQ does not allow this operation. */
export class NotAllowedError extends BaseError {
  constructor(message = "RabbitMQ does not allow this operation.", cause?: unknown) {
    super(message, 530, cause);
  }
}

/** 540: RabbitMQ does not support this operation. */
export class NotImplementedError extends BaseError {
  constructor(message = "RabbitMQ does not support this operation.", cause?: unknown) {
    super(message, 540, cause);
  }
}

/** 541: RabbitMQ encountered an internal error. */
export class InternalError extends BaseError {
  constructor(message = "RabbitMQ encountered an internal error.", cause?: unknown) {
    super(message, 541, cause);
  }
}

/** The received message does not match its Zod schema. */
export class ValidationError extends BaseError {
  constructor(cause?: unknown) {
    super("The message does not match the subscription schema.", "VALIDATION_ERROR", cause);
  }
}

/** A failed message could not be safely moved to its retry or dead-letter queue. */
export class FailureTransferError extends BaseError {
  constructor(cause?: unknown) {
    super("Could not confirm moving the failed message. The original was not acknowledged.", "FAILURE_TRANSFER_ERROR", cause);
  }
}
