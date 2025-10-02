import { closeSync, mkdirSync, openSync, writeSync } from 'node:fs';
import path from 'node:path';

import { CascLib, CascStorageFileEntry } from '../lib/cascLib';
import { expandMaskPatterns, normalizeStorageEntryPath, resolveStoragePath } from './utils';

const DEFAULT_OUTPUT_DIR = path.resolve(process.cwd(), 'output', 'extracted');
const DEFAULT_MASK = '*';
const DEFAULT_LIST_LIMIT = 1000;
const DEFAULT_CHUNK_SIZE = 1024 * 1024; // 1 MiB

type StageName = 'openStorage' | 'listFiles' | 'extractFile';

interface StageErrorContext {
  storagePath: string;
  outputDir: string;
  mask: string;
  limit: number | 'all';
  listFile?: string;
  entry?: string;
  activeMask?: string;
}

const STAGE_LABELS: Record<StageName, string> = {
  openStorage: 'Failed to open CASC storage',
  listFiles: 'Failed to enumerate CASC storage files',
  extractFile: 'Failed to extract file from CASC storage',
} as const;

function logStageError(stage: StageName, error: unknown, context: StageErrorContext): void {
  console.error(`${STAGE_LABELS[stage]}:`);
  console.error(error);
  console.error('  Storage:', context.storagePath);
  console.error('  Mask   :', context.mask);
  console.error('  Limit  :', context.limit);
  console.error('  Output :', context.outputDir);
  if (context.listFile) {
    console.error('  ListFile:', context.listFile);
  }
  if (context.entry) {
    console.error('  Entry:', context.entry);
  }
  process.exitCode = 1;
}

export interface ExtractStorageEntriesCommandOptions {
  storage?: string;
  mask?: string;
  limit?: string | number;
  listFile?: string;
  output?: string;
  overwrite?: boolean;
  chunkSize?: string | number;
}

function parseChunkSize(value: string | number | undefined): number {
  if (value === undefined) {
    return DEFAULT_CHUNK_SIZE;
  }

  if (typeof value === 'number' && Number.isFinite(value) && value > 0) {
    return Math.min(Math.floor(value), 0x7fff_ffff);
  }

  const numeric = Number.parseInt(String(value), 10);
  if (Number.isFinite(numeric) && numeric > 0) {
    return Math.min(numeric, 0x7fff_ffff);
  }

  console.warn(`Invalid chunk size "${value}", falling back to ${DEFAULT_CHUNK_SIZE}.`);
  return DEFAULT_CHUNK_SIZE;
}

function parseLimit(value: string | undefined): number {
  if (value === undefined) {
    return DEFAULT_LIST_LIMIT;
  }

  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return DEFAULT_LIST_LIMIT;
  }

  if (trimmed.toLowerCase() === 'all') {
    return Infinity;
  }

  const parsed = Number.parseInt(trimmed, 10);
  if (Number.isFinite(parsed) && parsed >= 0) {
    return parsed;
  }

  console.warn(`Invalid limit "${value}", falling back to ${DEFAULT_LIST_LIMIT}.`);
  return DEFAULT_LIST_LIMIT;
}

function normalizeLimitInput(value: string | number | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  return typeof value === 'number' ? value.toString() : value;
}

function ensureDirectoryForFile(filePath: string): void {
  const directory = path.dirname(filePath);
  mkdirSync(directory, { recursive: true });
}

function sanitizeRelativePath(entryPath: string): string {
  const normalized = normalizeStorageEntryPath(entryPath) || entryPath;
  const replaced = normalized.replace(/:/g, '/').replace(/[\\]+/g, '/');
  const segments = replaced
    .split('/')
    .map(segment => segment.trim())
    .filter(segment => segment.length > 0 && segment !== '.');

  const safeSegments = segments
    .map(segment => segment === '..' ? 'parent' : segment)
    .map(segment => segment.replace(/[<>:"|?*]/g, '_'));

  const relativePath = safeSegments.join(path.sep);
  return relativePath.length > 0 ? relativePath : 'extracted.bin';
}

function openStorageHandle(
  casc: CascLib,
  storagePath: string,
  context: StageErrorContext,
): bigint | null {
  try {
    const handle = casc.openStorage(storagePath);
    console.log('CASC storage opened:', handle.toString(16));
    return handle;
  } catch (error) {
    logStageError('openStorage', error, context);
    return null;
  }
}

function collectEntries(
  casc: CascLib,
  storageAddress: bigint,
  options: {
    mask: string;
    limitValue: number;
    listFile?: string;
    context: StageErrorContext;
  },
): CascStorageFileEntry[] | null {
  const masks = expandMaskPatterns(options.mask);
  const aggregated = new Map<string, CascStorageFileEntry>();
  let remaining = Number.isFinite(options.limitValue) ? options.limitValue : Infinity;

  for (const activeMask of masks) {
    if (remaining === 0) {
      break;
    }

    const listOptions: Parameters<CascLib['listStorageFiles']>[1] = { mask: activeMask };
    if (Number.isFinite(options.limitValue)) {
      listOptions.limit = remaining;
    }
    if (options.listFile !== undefined) {
      listOptions.listFile = options.listFile;
    }

    try {
      const entries = casc.listStorageFiles(storageAddress, listOptions);
      for (const entry of entries) {
        if (aggregated.has(entry.path)) {
          continue;
        }
        aggregated.set(entry.path, entry);
        if (Number.isFinite(options.limitValue)) {
          remaining -= 1;
          if (remaining === 0) {
            break;
          }
        }
      }
    } catch (error) {
      logStageError('listFiles', error, { ...options.context, activeMask });
      return null;
    }
  }

  return Array.from(aggregated.values());
}

function extractEntries(
  casc: CascLib,
  storageAddress: bigint,
  entries: CascStorageFileEntry[],
  options: {
    outputDir: string;
    overwrite: boolean;
    chunkSize: number;
    context: StageErrorContext;
  },
): number {
  let extractedCount = 0;

  for (const entry of entries) {
    if (entry.path.endsWith(':')) {
      continue;
    }

    let fileHandle: bigint | null = null;
    const entryContext = { ...options.context, entry: entry.path };

    try {
      fileHandle = casc.openFile(storageAddress, entry.path);
    } catch (error) {
      logStageError('extractFile', error, entryContext);
      continue;
    }

    try {
      const fileSizeBig = casc.getFileSize(fileHandle);
      let remaining = fileSizeBig;
      const destinationRelative = sanitizeRelativePath(entry.path);
      const destinationPath = path.resolve(options.outputDir, destinationRelative);
      ensureDirectoryForFile(destinationPath);

      const flags = options.overwrite ? 'w' : 'wx';
      let fileDescriptor: number | null = null;
      try {
        fileDescriptor = openSync(destinationPath, flags);
      } catch (error) {
        if (!options.overwrite && (error as NodeJS.ErrnoException)?.code === 'EEXIST') {
          console.warn(`Skipped existing file without overwrite: ${destinationRelative}`);
          continue;
        }
        console.error(`Failed to open destination path "${destinationPath}" (${flags}).`);
        console.error(error);
        process.exitCode = 1;
        continue;
      }

      try {
        const buffer = Buffer.alloc(Math.min(options.chunkSize, 0x7fff_ff00));
        while (remaining > 0n) {
          const toRead = remaining < BigInt(buffer.length) ? Number(remaining) : buffer.length;
          const bytesRead = casc.readFile(fileHandle, buffer, toRead);
          if (bytesRead === 0) {
            break;
          }
          writeSync(fileDescriptor, buffer, 0, bytesRead);
          remaining -= BigInt(bytesRead);
        }

        if (remaining === 0n) {
          extractedCount += 1;
          console.log(`Extracted ${destinationRelative} (${fileSizeBig} bytes)`);
        } else {
          console.warn(`Incomplete extraction for ${destinationRelative}: ${remaining} bytes remaining.`);
          process.exitCode = 1;
        }
      } finally {
        if (fileDescriptor !== null) {
          closeSync(fileDescriptor);
        }
      }
    } catch (error) {
      logStageError('extractFile', error, entryContext);
    } finally {
      if (fileHandle !== null) {
        try {
          casc.closeFile(fileHandle);
        } catch (closeError) {
          console.error('Failed to close CASC file handle:', closeError);
        }
      }
    }
  }

  return extractedCount;
}

export function extractStorageEntries({
  storage,
  mask: maskInput,
  limit: rawLimit,
  listFile: listFileInput,
  output: outputInput,
  overwrite = false,
  chunkSize: chunkSizeInput,
}: ExtractStorageEntriesCommandOptions = {}): void {
  const storagePath = resolveStoragePath(storage);
  if (!storagePath) {
    return;
  }

  const sanitizedMask = maskInput?.trim();
  const mask = sanitizedMask && sanitizedMask.length > 0 ? sanitizedMask : DEFAULT_MASK;

  const limitArg = normalizeLimitInput(rawLimit);
  const trimmedLimitArg = limitArg?.trim();
  const limitValue = parseLimit(trimmedLimitArg);

  const listFile = listFileInput && listFileInput.trim().length > 0 ? listFileInput.trim() : undefined;

  const outputDir = outputInput && outputInput.trim().length > 0
    ? path.resolve(process.cwd(), outputInput.trim())
    : DEFAULT_OUTPUT_DIR;
  mkdirSync(outputDir, { recursive: true });

  const chunkSize = parseChunkSize(chunkSizeInput);

  const casc = new CascLib(process.env.CASC_LIBRARY_PATH ?? path.resolve(__dirname, '../../../casc.framework/casc'));
  let storageAddress: bigint | null = null;

  const errorContextBase: StageErrorContext = {
    storagePath,
    outputDir,
    mask,
    limit: Number.isFinite(limitValue) ? limitValue : 'all',
    listFile,
  };

  try {
    const handle = openStorageHandle(casc, storagePath, errorContextBase);
    if (handle === null) {
      return;
    }
    storageAddress = handle;

    const entries = collectEntries(casc, handle, {
      mask,
      limitValue,
      listFile,
      context: errorContextBase,
    });
    if (entries === null) {
      return;
    }

    if (entries.length === 0) {
      console.warn('No matching files found for the provided mask.');
      return;
    }

    console.log(`Preparing to extract ${entries.length} file(s) to ${outputDir}`);
    const extractedCount = extractEntries(casc, handle, entries, {
      outputDir,
      overwrite,
      chunkSize,
      context: errorContextBase,
    });

    console.log(`Extraction complete. ${extractedCount}/${entries.length} file(s) written.`);
  } finally {
    if (storageAddress !== null) {
      try {
        casc.closeStorage(storageAddress);
      } catch (closeError) {
        console.error('Failed to close CASC storage:', closeError);
      }
    }
  }
}
