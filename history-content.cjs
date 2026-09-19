// Read-only enrichment of legacy text-only transcripts. Never rewrite the native
// log or invent native IDs. Refuse ambiguous order/content rather than splice
// unrelated branches into a conversation.
const fs = require('node:fs');
function nativeMessages(ref) {
  const raw = fs.readFileSync(ref, 'utf8');
  const rows = raw.split('\n');
  if (rows.at(-1) === '') rows.pop(); else rows.pop(); // incomplete trailing record is not committed
  const entries = rows.filter(Boolean).map(line => JSON.parse(line));
  const ids = new Map(entries.filter(e=>e.id).map(e=>[e.id,e]));
  const branch = []; const seen = new Set();
  let entry = [...entries].reverse().find(e=>e.id);
  while (entry) {
    if (seen.has(entry.id)) throw new Error('invalid native history cycle');
    seen.add(entry.id); branch.push(entry);
    if (!entry.parentId) break;
    entry = ids.get(entry.parentId);
    if (!entry) throw new Error('incomplete native history ancestry');
  }
  return branch.reverse().filter(e=>e.message);
}
function enrichHistory(ref, transcript) {
  if (!fs.existsSync(ref)) return transcript;
  const branch = nativeMessages(ref);
  const native = branch.filter(e=>['user','assistant'].includes(e.message.role));
  const textOf = m => (m.content || []).filter(p=>p.type==='text').map(p=>p.text || '').join('');
  const matches = (entry, record) => record.nativeId ? entry.id === record.nativeId
    : entry.message.role === record.role && textOf(entry.message) === (record.text || '');
  // A harness may insert its own user-role continuations. Align the adapter's
  // transcript as a subsequence, but accept only an unambiguous alignment.
  const earliest = [], latest = []; let cursor = 0;
  for (const record of transcript) {
    while (cursor < native.length && !matches(native[cursor], record)) cursor++;
    if (cursor === native.length) throw new Error('history alignment unresolved; transcript message missing');
    earliest.push(cursor++);
  }
  cursor = native.length - 1;
  for (let i = transcript.length - 1; i >= 0; i--) {
    while (cursor >= 0 && !matches(native[cursor], transcript[i])) cursor--;
    if (cursor < 0) throw new Error('history alignment unresolved; transcript message missing');
    latest[i] = cursor--;
  }
  if (earliest.some((index,i) => index !== latest[i])) throw new Error('history alignment unresolved; ambiguous native mapping');
  const mapped = new Map(earliest.map((index,i)=>[index, transcript[i]]));
  const results = new Map(branch.filter(e=>e.message.role==='toolResult').map(e=>[e.message.toolCallId,e.message]));
  return native.map((entry,index)=>{
    // Preserve known public IDs; expose additional native records by their own
    // stable IDs, not guessed positions or fabricated replacement messages.
    const record = mapped.get(index) || { id: entry.id, role: entry.message.role, text: textOf(entry.message), complete: true, source: 'harness' };
    const parts=entry.message.content || [];
    return {...record, nativeId:entry.id, createdAt:entry.timestamp,
      reasoning:parts.filter(p=>p.type==='thinking').map(p=>p.thinking || '').join('\n'),
      content:parts.filter(p=>p.type==='text'||p.type==='image'),
      tools:parts.filter(p=>p.type==='toolCall').map(p=>{
        const result=results.get(p.id);
        return {id:p.id,name:p.name,args:p.arguments || {},state:result ? result.isError ? 'failed':'done':'pending',
          ...(result?{result:result.content}: {})};
      })};
  });
}
module.exports={enrichHistory};
