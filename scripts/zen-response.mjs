// Turn a provider response into the persisted zen-pool result contract.
// A provider's HTTP 200 is not a usable answer when both text and tool_calls are empty.
export function normalizeZenResponse(res, model) {
  const text = res?.ok ? String(res.message?.content ?? '').trim() : '';
  const toolCalls = res?.ok && Array.isArray(res.message?.tool_calls) && res.message.tool_calls.length
    ? res.message.tool_calls : null;
  const ok = !!res?.ok && (text.length > 0 || !!toolCalls);
  const empty = !!res?.ok && !ok;
  return {
    ok,
    text,
    kind: empty ? 'empty' : (res?.ok ? 'ok' : String(res?.kind || 'error')),
    error: empty
      ? `empty answer from ${String(model || 'unknown model').slice(0, 160)} (0 text chars, no tool calls)`
      : (res?.ok ? null : String(res?.error || res?.bodySnippet || '').slice(0, 500)),
    tool_calls: toolCalls,
  };
}
