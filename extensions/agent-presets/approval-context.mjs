import fs from 'node:fs/promises';
import path from 'node:path';
const MAX_PREVIEW = 512 * 1024;
// Read-only preview, not a replacement implementation of the harness's edit tool.
export async function approvalContext(event, cwd) {
  const input = event.input && typeof event.input === 'object' ? event.input : {};
  const result = { schema: 'tool-review/v1', tool: event.toolName, toolCallId: event.toolCallId, cwd, input };
  if (typeof input.command === 'string') result.command = input.command;
  if (typeof input.plan === 'string') result.plan = input.plan;
  if (typeof input.path === 'string') result.path = path.resolve(cwd, input.path);
  if (event.toolName === 'write' && result.path && typeof input.content === 'string') {
    if (Buffer.byteLength(input.content) > MAX_PREVIEW) {
      result.previewNote = '拟写入内容超过预览大小限制'; return result;
    }
    try {
      const stat = await fs.stat(result.path);
      if (!stat.isFile() || stat.size > MAX_PREVIEW) { result.previewNote = '原文件无法完整预览'; return result; }
      const bytes = await fs.readFile(result.path);
      if (bytes.includes(0)) { result.previewNote = '原文件不是可预览的文本文件'; return result; }
      result.before = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
      result.change = 'overwrite';
    } catch (error) {
      if (error.code !== 'ENOENT') { result.previewNote = '无法读取修改前的文件'; return result; }
      result.before = ''; result.change = 'create';
    }
    result.after = input.content;
    result.previewNote = '审批时读取的文件快照；其他进程仍可能修改此文件。';
  } else if (event.toolName === 'edit' && typeof input.oldText === 'string' && typeof input.newText === 'string') {
    result.before = input.oldText; result.after = input.newText;
    result.change = 'fragment';
    result.previewNote = '仅比较请求中的替换片段，不代表完整文件或已应用的修改。';
  }
  if (event.toolName === 'edit' && Array.isArray(input.edits)) {
    result.edits = input.edits.filter(e => typeof e?.oldText === 'string' && typeof e?.newText === 'string').map(e => ({ before: e.oldText, after: e.newText }));
    result.change = 'fragments';
    result.previewNote = '逐项比较请求中的替换片段，不代表完整文件或已执行的修改。';
  }
  return result;
}
