import { statSync } from 'node:fs';
import path from 'node:path';

const STORAGE_HINT = '❗ 请确认路径指向有效的 CASC 存储根目录，例如包含 Data/ 或 .build.info 的文件夹';

export function resolveStoragePath(rawInput: string | undefined): string | null {
  const trimmed = rawInput?.trim();
  if (!trimmed) {
    console.error('Missing required storage path. Provide --storage or positional <storage>.');
    process.exitCode = 1;
    return null;
  }

  const resolved = path.resolve(trimmed);

  try {
    const stats = statSync(resolved);
    if (!stats.isDirectory()) {
      console.error('Storage path is not a directory:', resolved);
      console.error(`  ${STORAGE_HINT}`);
      process.exitCode = 1;
      return null;
    }
  } catch (error) {
    console.error('Unable to access storage path:', resolved);
    console.error(`  ${STORAGE_HINT}`);
    console.error('  原因:', error instanceof Error ? error.message : error);
    process.exitCode = 1;
    return null;
  }

  return resolved;
}

export function normalizeStorageEntryPath(entry: string): string {
  const slashNormalized = entry.replace(/[\\]+/g, '/');
  const trimmed = slashNormalized.replace(/^\/+|\/+$/g, '');
  if (trimmed.startsWith('vfs-root/')) {
    return trimmed.slice('vfs-root/'.length);
  }
  if (trimmed === 'vfs-root') {
    return '';
  }
  return trimmed;
}

function splitBraceAlternatives(segment: string): string[] {
  const parts: string[] = [];
  let current = '';
  let depth = 0;

  for (let index = 0; index < segment.length; index += 1) {
    const char = segment[index];
    if (char === ',' && depth === 0) {
      parts.push(current);
      current = '';
    } else {
      if (char === '{') {
        depth += 1;
      } else if (char === '}') {
        depth = Math.max(0, depth - 1);
      }
      current += char;
    }
  }

  parts.push(current);
  return parts;
}

export function expandMaskPatterns(pattern: string): string[] {
  const start = pattern.indexOf('{');
  if (start === -1) {
    return [pattern];
  }

  let depth = 0;
  let end = -1;
  for (let index = start + 1; index < pattern.length; index += 1) {
    const char = pattern[index];
    if (char === '{') {
      depth += 1;
    } else if (char === '}') {
      if (depth === 0) {
        end = index;
        break;
      }
      depth -= 1;
    }
  }

  if (end === -1) {
    return [pattern];
  }

  const prefix = pattern.slice(0, start);
  const body = pattern.slice(start + 1, end);
  const suffix = pattern.slice(end + 1);

  const alternatives = splitBraceAlternatives(body);
  const results = new Set<string>();

  alternatives.forEach(option => {
    expandMaskPatterns(`${prefix}${option}${suffix}`).forEach(expanded => {
      results.add(expanded);
    });
  });

  return Array.from(results);
}
