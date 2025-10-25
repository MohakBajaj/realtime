import type { Opts, Realtime } from "./realtime.js";
import type { SystemEvent, UserEvent } from "../types.js";

/**
 * Parses ioredis XRANGE/XREVRANGE result format to object format
 * ioredis returns: [[id, [field1, value1, field2, value2, ...]], ...]
 * We convert to: { [id]: { field1: value1, field2: value2, ... } }
 */
function parseStreamResult(
  result: Array<[string, string[]]>
): Record<string, Record<string, any>> {
  const parsed: Record<string, Record<string, any>> = {};

  for (const [id, fields] of result) {
    const entry: Record<string, any> = {};
    for (let i = 0; i < fields.length; i += 2) {
      const field = fields[i];
      const value = fields[i + 1];

      if (field !== undefined && value !== undefined) {
        try {
          entry[field] = JSON.parse(value);
        } catch {
          entry[field] = value;
        }
      }
    }
    parsed[id] = entry;
  }

  return parsed;
}

export function handle<T extends Opts>(config: {
  realtime: Realtime<T>;
  middleware?: ({
    request,
    channels,
  }: {
    request: Request;
    channels: string[];
  }) => Response | void | Promise<Response | void>;
}): (request: Request) => Promise<Response | void> {
  return async (request: Request) => {
    const requestStartTime = Date.now();
    const { searchParams } = new URL(request.url);
    const channels =
      searchParams.getAll("channels").length > 0
        ? searchParams.getAll("channels")
        : ["default"];
    const reconnect = searchParams.get("reconnect");
    const history_all = searchParams.get("history_all");
    const history_length = searchParams.get("history_length");
    const history_since = searchParams.get("history_since");
    const connection_start = searchParams.get("connection_start");

    const lastAckMap = new Map<string, string>();
    channels.forEach((channel) => {
      const lastAck = searchParams.get(`last_ack_${channel}`);
      if (lastAck) {
        lastAckMap.set(channel, lastAck);
      }
    });

    const redis = config.realtime._redis;
    const logger = config.realtime._logger;

    if (config.middleware) {
      const result = await config.middleware({ request, channels });
      if (result) return result;
    }

    if (!redis) {
      logger.error("No Redis instance provided to Realtime");
      return new Response(JSON.stringify({ error: "Redis not configured" }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }

    let cleanup: (() => Promise<void>) | undefined;
    let subscriber: ReturnType<typeof redis.duplicate>;
    let reconnectTimeout: NodeJS.Timeout | undefined;
    let keepaliveInterval: NodeJS.Timeout | undefined;
    let isClosed = false;
    let handleAbort: (() => Promise<void>) | undefined;
    let onSubscribe: (() => Promise<void>) | undefined;
    let onError: ((err: Error) => void) | undefined;
    let onMessage:
      | ((channel: string, message: string) => Promise<void>)
      | undefined;

    const stream = new ReadableStream({
      async start(controller) {
        if (request.signal.aborted) {
          controller.close();
          return;
        }

        cleanup = async () => {
          if (isClosed) return;
          isClosed = true;

          clearTimeout(reconnectTimeout);
          clearInterval(keepaliveInterval);

          if (handleAbort) {
            request.signal.removeEventListener("abort", handleAbort);
          }

          if (subscriber) {
            try {
              await subscriber.unsubscribe();
              subscriber.disconnect();
            } catch (err) {
              logger.error("⚠️ Error during unsubscribe:", err);
            }
          }

          controller.close();
        };

        handleAbort = async () => {
          await cleanup?.();
        };

        request.signal.addEventListener("abort", handleAbort);

        subscriber = redis.duplicate();
        await new Promise<void>((resolve, reject) => {
          subscriber.subscribe(
            ...channels.map((ch) => `channel:${ch}`),
            (err) => {
              if (err) reject(err);
              else resolve();
            }
          );
        });

        const safeEnqueue = (data: Uint8Array) => {
          if (!isClosed) controller.enqueue(data);
        };

        const elapsedMs = Date.now() - requestStartTime;
        const remainingMs = config.realtime._maxDurationSecs * 1000 - elapsedMs;
        const streamDurationMs = Math.max(remainingMs - 2000, 1000);

        reconnectTimeout = setTimeout(async () => {
          safeEnqueue(json({ type: "reconnect" }));
          await cleanup?.();
        }, streamDurationMs);

        onSubscribe = async () => {
          logger.log(`✅ Subscription established:`, { channels });
          try {
            for (const channel of channels) {
              const last_ack = lastAckMap.get(channel);

              if (reconnect === "true" && last_ack) {
                const startId = `(${last_ack}`;
                const rawMissingMessages = await redis.xrange(
                  `channel:${channel}`,
                  startId,
                  "+"
                );

                const missingMessages = parseStreamResult(rawMissingMessages);

                const connectedEvent: SystemEvent = {
                  type: "connected",
                  channel,
                };

                safeEnqueue(json(connectedEvent));

                if (Array.from(Object.keys(missingMessages)).length > 0) {
                  Object.entries(missingMessages).forEach(
                    ([__stream_id, value]) => {
                      if (typeof value === "object" && value !== null) {
                        const { __event_path, data } = value as Record<
                          string,
                          unknown
                        >;
                        const userEvent: UserEvent = {
                          data,
                          __event_path: __event_path as string[],
                          __stream_id,
                          __channel: channel,
                        };
                        safeEnqueue(json(userEvent));
                      }
                    }
                  );
                }
              } else {
                const connectionStart = connection_start ?? String(Date.now());

                if (history_all || history_length || history_since) {
                  let start = history_since ? history_since : "-";
                  let count = history_length
                    ? parseInt(history_length, 10)
                    : undefined;

                  if (
                    history_since &&
                    parseInt(history_since, 10) > parseInt(connectionStart, 10)
                  ) {
                    start = connectionStart;
                  }

                  const rawHistory =
                    count !== undefined
                      ? await redis.xrevrange(
                          `channel:${channel}`,
                          "+",
                          start,
                          "COUNT",
                          count
                        )
                      : await redis.xrevrange(`channel:${channel}`, "+", start);

                  const history = parseStreamResult(rawHistory);
                  const messages = Object.entries(history);
                  const currentCursorId = messages[0]?.[0] ?? "0-0";
                  const oldestToNewestMessages = reverse(messages);

                  const connectedEvent: SystemEvent = {
                    type: "connected",
                    channel,
                    cursor: currentCursorId,
                  };

                  safeEnqueue(json(connectedEvent));

                  oldestToNewestMessages.forEach(([__stream_id, value]) => {
                    if (typeof value === "object" && value !== null) {
                      const { __event_path, data } = value as Record<
                        string,
                        unknown
                      >;
                      const userEvent: UserEvent = {
                        data,
                        __event_path: __event_path as string[],
                        __stream_id,
                        __channel: channel,
                      };
                      safeEnqueue(json(userEvent));
                    }
                  });
                } else {
                  const rawHistory = await redis.xrevrange(
                    `channel:${channel}`,
                    "+",
                    connectionStart
                  );

                  const history = parseStreamResult(rawHistory);
                  const messages = Object.entries(history);
                  const currentCursorId = messages[0]?.[0] ?? "0-0";
                  const oldestToNewestMessages = reverse(messages);

                  const connectedEvent: SystemEvent = {
                    type: "connected",
                    channel,
                    cursor: currentCursorId,
                  };

                  safeEnqueue(json(connectedEvent));

                  oldestToNewestMessages.forEach(([__stream_id, value]) => {
                    if (typeof value === "object" && value !== null) {
                      const { __event_path, data } = value as Record<
                        string,
                        unknown
                      >;
                      const userEvent: UserEvent = {
                        data,
                        __event_path: __event_path as string[],
                        __stream_id,
                        __channel: channel,
                      };
                      safeEnqueue(json(userEvent));
                    }
                  });
                }
              }
            }
          } catch (err) {
            logger.error("Error in subscribe handler:", err);
            safeEnqueue(
              json({
                type: "error",
                error: err instanceof Error ? err.message : "Unknown error",
              })
            );
          }
        };

        onError = (err: Error) => {
          logger.error("⚠️ Redis subscriber error:", err);

          const errorEvent: SystemEvent = {
            type: "error",
            error: err.message,
          };

          safeEnqueue(json(errorEvent));
        };

        onMessage = async (redisChannel: string, message: string) => {
          const channel = redisChannel.replace(/^channel:/, "");

          let payload: Record<string, unknown>;
          try {
            payload = JSON.parse(message);
          } catch {
            payload = { data: message };
          }

          if (payload.type === "ping") {
            const pingEvent: SystemEvent = {
              type: "ping",
              timestamp: payload.timestamp as number,
            };
            safeEnqueue(json(pingEvent));
            return;
          }

          const { __stream_id, __event_path, data } = payload;

          logger.log("⬇️  Received event:", { channel, __event_path, data });

          const userEvent: UserEvent = {
            data,
            __event_path: __event_path as string[],
            __stream_id: __stream_id as string,
            __channel: channel,
          };

          safeEnqueue(json(userEvent));
        };

        onSubscribe().catch((err) => {
          logger.error("Error in subscribe handler:", err);
        });
        subscriber.on("error", onError);
        subscriber.on("message", onMessage);

        keepaliveInterval = setInterval(async () => {
          for (const channel of channels) {
            await redis.publish(
              `channel:${channel}`,
              JSON.stringify({
                type: "ping",
                timestamp: Date.now(),
              })
            );
          }
        }, 10_000);
      },

      async cancel() {
        if (isClosed) return;
        await cleanup?.();
      },
    });

    return new StreamingResponse(stream);
  };
}

export function json<T>(data: SystemEvent | UserEvent<T>) {
  return new TextEncoder().encode(`data: ${JSON.stringify(data)}\n\n`);
}

export class StreamingResponse extends Response {
  constructor(res: ReadableStream<any>, init?: ResponseInit) {
    super(res as any, {
      ...init,
      status: 200,
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Cache-Control",
        ...init?.headers,
      },
    });
  }
}

function reverse(array: Array<any>) {
  const length = array.length;

  let left = null;
  let right = null;

  for (left = 0, right = length - 1; left < right; left += 1, right -= 1) {
    const temporary = array[left];
    array[left] = array[right];
    array[right] = temporary;
  }

  return array;
}
