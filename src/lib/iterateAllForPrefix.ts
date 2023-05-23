export async function iterateAllForPrefix(
  kvStore: KVNamespace,
  prefix: string,
  func: (key: string, value: ReadableStream<any> | null) => Promise<void>
) {
  let cursor: string | undefined;
  do {
    const list: { cursor?: string; keys: any[] } = await kvStore.list({
      prefix,
      cursor,
    });
    cursor = list.cursor;
    for (const key of list.keys) {
      const stream = await kvStore.get(key.name, "stream");
      await func(key, stream);
    }
  } while (cursor);
}
