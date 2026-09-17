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
  if (native.length !== transcript.length || !native.every((e,i)=>e.message.role===transcript[i].role && textOf(e.message)===(transcript[i].text || ''))) {
    // New records with explicit native mapping can still be enriched safely.
    if (!transcript.every(e=>e.nativeId)) throw new Error('history alignment unresolved; refusing text-only replacement');
  }
  const results = new Map(branch.filter(e=>e.message.role==='toolResult').map(e=>[e.message.toolCallId,e.message]));
  return transcript.map((record,index)=>{
    const entry = record.nativeId ? native.find(e=>e.id===record.nativeId) : native[index];
    if (!entry || entry.message.role!==record.role) throw new Error('native history message missing');
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
