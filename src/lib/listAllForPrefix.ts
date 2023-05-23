export async function listAllForPrefix(kvStore: KVNamespace, prefix: string) {
  const keys = [];
  let cursor: string | undefined;
  do {
    const list: { cursor?: string; keys: any[] } = await kvStore.list({
      prefix,
      cursor,
    });
    cursor = list.cursor;
    keys.push(...list.keys);
  } while (cursor);
  return keys;
}
