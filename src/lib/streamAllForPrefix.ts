import { iterateAllForPrefix } from "./iterateAllForPrefix";

export async function streamAllForPrefix(
  kvStore: KVNamespace,
  prefix: string,
  targetWritableStream: WritableStream<any>
) {
  let length = 0;
  const writer = await targetWritableStream.getWriter();
  writer.write(Uint8Array.from(["[".charCodeAt(0)]));
  length += 1;

  let index = 0;
  await iterateAllForPrefix(kvStore, prefix, async (key, stream) => {
    if (stream) {
      if (index > 0) {
        await writer.write(Uint8Array.from([",".charCodeAt(0)]));
        length += 1;
      }
      const reader = await stream.getReader();
      await reader
        .read()
        .then(async function readChunk({ done, value }): Promise<any> {
          if (done) {
            return;
          }
          await writer.write(value);
          length += value.length;
          return reader.read().then(readChunk);
        });
      index += 1;
    }
  });

  await writer.write(Uint8Array.from(["]".charCodeAt(0)]));
  length += 1;
  await writer.close();
  return length;
}
