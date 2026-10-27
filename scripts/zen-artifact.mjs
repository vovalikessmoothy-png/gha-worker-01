// CI artifacts must contain operations data only. Never serialize prompts, model text, tool
// arguments, provider errors, or other request/response payloads from a pool call.
const OPERATIONAL_FIELDS = [
  'ok', 'model', 'kind', 'stopped_by', 'status', 'provider_ms', 'served_ms',
  'usage', 'finish_reason', 'runner', 'marks', 'served_at', 'rotate',
];

export function sanitizePoolArtifact(record) {
  return Object.fromEntries(OPERATIONAL_FIELDS
    .filter((key) => record[key] !== undefined)
    .map((key) => [key, record[key]]));
}
