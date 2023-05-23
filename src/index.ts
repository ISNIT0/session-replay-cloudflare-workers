import { Router } from "itty-router";
import { error } from "itty-router-extras";
import { createCors } from "itty-cors";
import { listAllForPrefix } from "./lib/listAllForPrefix";
import { streamAllForPrefix } from "./lib/streamAllForPrefix";

const FLUSH_TIMEOUT = 1000 * 60; // 1 hour

export interface Env {
  // Example binding to KV. Learn more at https://developers.cloudflare.com/workers/runtime-apis/kv/
  SESSION_CACHE_KV: KVNamespace;
  //
  // Example binding to Durable Object. Learn more at https://developers.cloudflare.com/workers/runtime-apis/durable-objects/
  // MY_DURABLE_OBJECT: DurableObjectNamespace;
  //
  // Example binding to R2. Learn more at https://developers.cloudflare.com/workers/runtime-apis/r2/
  SESSION_BUCKET_R2: R2Bucket;
  //
  // Example binding to a Service. Learn more at https://developers.cloudflare.com/workers/runtime-apis/service-bindings/
  //   MY_SERVICE: Fetcher;
}

const { preflight, corsify } = createCors();

// Create a new router
const router = Router();

router.all("*", preflight as any);

router.get("/", async (request, env: Env) => {
  const keys = await listAllForPrefix(env.SESSION_CACHE_KV, "");

  return new Response(JSON.stringify(keys), {
    headers: {
      "Content-Type": "application/json",
    },
  });
});

router.get("/session/:sessionId", async (request, env: Env) => {
  let { readable, writable } = new TransformStream();

  streamAllForPrefix(env.SESSION_CACHE_KV, request.params.sessionId, writable);

  return new Response(readable, {
    headers: {
      "Content-Type": "application/json",
    },
  });
});

router.get("/session/:sessionId/keys", async (request, env: Env) => {
  const keys = await listAllForPrefix(
    env.SESSION_CACHE_KV,
    request.params.sessionId
  );

  return new Response(JSON.stringify(keys), {
    headers: {
      "Content-Type": "application/json",
    },
  });
});

router.post("/session/:sessionId", async (request, env: Env) => {
  const { sessionId } = request.params;

  let fields = {
    asn: request.cf.asn,
    colo: request.cf.colo,
  };

  if (request.headers.get("Content-Type") === "application/json") {
    const json = await request.json();
    const events = json.events;

    // TODO: Pre-process JSON

    if (events) {
      console.log(
        `Received [${events.length}] events for session [${sessionId}]`
      );
      const eventsStr = JSON.stringify(events).slice(1, -1);
      await env.SESSION_CACHE_KV.put(`${sessionId}:${Date.now()}`, eventsStr);
    }
  }

  const returnData = JSON.stringify(fields, null, 2);

  return new Response(returnData, {
    headers: {
      "Content-Type": "application/json",
    },
  });
});

// TODO: Doesn't work yet
router.post("/flush", async (request, env: Env) => {
  const flushableTs = Date.now() - FLUSH_TIMEOUT;
  const acc: Record<string, number> = {};
  let cursor: string | undefined;
  do {
    const list: { cursor?: string; keys: any[] } =
      await env.SESSION_CACHE_KV.list({
        cursor,
      });
    cursor = list.cursor;

    for (const key of list.keys) {
      const [sessionId, _timestamp] = key.name.split(":");
      if (sessionId && _timestamp) {
        const timestamp = Number(_timestamp);
        if (timestamp < flushableTs) {
          acc[sessionId] = timestamp;
        } else {
          delete acc[sessionId];
        }
      }
    }
  } while (cursor);

  const sessionsToFlush = Object.keys(acc);
  console.log(`Got [${sessionsToFlush.length}] sessions to flush`);

  for (const sessionId of sessionsToFlush) {
    console.info(`Flushing session [${sessionId}]`);
    // TODO: Flush session to R2
    // const upload = await env.SESSION_BUCKET_R2.createMultipartUpload(sessionId);

    // let partNumber = 1;
    // let parts: R2UploadedPart[] = [];
    // await iterateAllForPrefix(
    //   env.SESSION_CACHE_KV,
    //   sessionId,
    //   async (key, stream) => {
    //     if (stream) {
    //       const part = await upload.uploadPart(partNumber, stream);
    //       parts.push(part);
    //       partNumber += 1;
    //     }
    //   }
    // );
    // await upload.complete(parts);

    // Remove from KV
    const keys = await listAllForPrefix(env.SESSION_CACHE_KV, sessionId);
    for (const key of keys) {
      // Not Awaiting
      env.SESSION_CACHE_KV.delete(key.name);
    }
  }

  const returnData = JSON.stringify(
    { flushed: sessionsToFlush.length },
    null,
    2
  );

  return new Response(returnData, {
    headers: {
      "Content-Type": "application/json",
    },
  });
});

router.all("*", () => new Response("404, not found!", { status: 404 }));

export default {
  fetch: (...args: any[]) =>
    (router as any)
      .handle(...args)
      .catch((err: any) => error(500, err.stack))
      .then(corsify),
};
