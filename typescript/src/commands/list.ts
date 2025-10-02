import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';

import { CascLib, CascStorageFileEntry } from '../lib/cascLib';
import { expandMaskPatterns, resolveStoragePath } from './utils';
const DEFAULT_MASK = '*';
const DEFAULT_LIST_LIMIT = 1000;
const LIMIT_LABEL_ALL = 'all';

function printStorageInfo(info: ReturnType<CascLib['getStorage']>): void {
  console.log('Storage info [LocalFileCount]:', info.localFileCount);
  console.log('Storage info [TotalFileCount]:', info.totalFileCount);
  console.log('Storage info [Features]:', `${info.features.value} [${info.features.features.join(', ')}]`);
  console.log('Storage info [Product]:', `${info.product.codeName} (build ${info.product.buildNumber})`);
  console.log('Storage info [Tags]:', info.tags.map(tag => `${tag.name}=${tag.value}`).join(', '));
  console.log('Storage info [PathProduct]:', info.pathProduct);
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

type StageName = 'openStorage' | 'getStorage' | 'listStorageFiles';

interface StageErrorContext {
  storagePath: string;
  mask: string;
  maskInput?: string;
  limitValue: number;
  limitInput?: string;
  listFile?: string;
  activeMask?: string;
}

const STAGE_LABELS: Record<StageName, string> = {
  openStorage: 'Failed to open CASC storage',
  getStorage: 'Failed to query CASC storage info',
  listStorageFiles: 'Failed to enumerate CASC storage files',
} as const;

function formatLimitLabel(limit: number): string {
  return Number.isFinite(limit) ? limit.toString() : LIMIT_LABEL_ALL;
}

function logStageError(stage: StageName, error: unknown, context: StageErrorContext): void {
  console.error(`${STAGE_LABELS[stage]}:`);
  console.error(error);
  console.error('  Path:', context.storagePath);

  if (stage === 'openStorage') {
    if (context.maskInput !== undefined) {
      console.error('  Mask:', context.mask);
    }
    if (context.limitInput !== undefined) {
      console.error('  Limit:', context.limitInput);
    }
    if (context.listFile !== undefined) {
      console.error('  ListFile:', context.listFile);
    }
    console.error('  ❗ 请确认路径指向有效的 CASC 存储根目录，例如包含 Data/ 或 .build.info 的文件夹');
  } else if (stage === 'listStorageFiles') {
    console.error('  Mask:', context.activeMask ?? context.mask);
    console.error('  Limit:', context.limitInput ?? formatLimitLabel(context.limitValue));
    if (context.listFile !== undefined) {
      console.error('  ListFile:', context.listFile);
    }
  }

  console.error('  GetLastError():', error instanceof Error ? error.message : error);
  process.exitCode = 1;
}

function printFileListing(
  files: CascStorageFileEntry[],
  mask: string,
  limit: number,
  listFile?: string,
): void {
  const limitLabel = Number.isFinite(limit) ? limit.toString() : 'unbounded';
  const listFileLabel = listFile ? `, listFile="${listFile}"` : '';

  console.log('');
  console.log(`Listing storage entries (mask="${mask}", limit=${limitLabel}${listFileLabel}):`);

  if (limit === 0) {
    console.log('  Skipped: limit=0.');
    return;
  }

  if (files.length === 0) {
    console.log('  <no entries>');
    return;
  }

  files.forEach((entry, index) => {
    const ordinal = String(index + 1).padStart(3, ' ');
    console.log(`  ${ordinal}. ${entry.path} (${entry.size} bytes)`);
  });

  if (Number.isFinite(limit) && files.length === limit) {
    console.log('  ... limit reached');
  }
}

export interface ListStorageFilesCommandOptions {
  storage?: string;
  mask?: string;
  limit?: string | number;
  listFile?: string;
  json?: boolean;
  output?: string;
}

function normalizeLimitInput(value: string | number | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }

  return typeof value === 'number' ? value.toString() : value;
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

function fetchStorageInfo(
  casc: CascLib,
  handle: bigint,
  context: StageErrorContext,
): ReturnType<CascLib['getStorage']> | null {
  try {
    return casc.getStorage(handle);
  } catch (error) {
    logStageError('getStorage', error, context);
    return null;
  }
}

function aggregateFileEntries(
  casc: CascLib,
  storageAddress: bigint,
  options: {
    mask: string;
    limit: number;
    listFile?: string;
    context: StageErrorContext;
  },
): CascStorageFileEntry[] | null {
  const masks = expandMaskPatterns(options.mask);
  const results = new Map<string, CascStorageFileEntry>();
  let remaining = Number.isFinite(options.limit) ? options.limit : Infinity;

  for (const activeMask of masks) {
    if (remaining === 0) {
      break;
    }

    const queryOptions: Parameters<CascLib['listStorageFiles']>[1] = { mask: activeMask };
    if (Number.isFinite(options.limit)) {
      queryOptions.limit = remaining;
    }
    if (options.listFile !== undefined) {
      queryOptions.listFile = options.listFile;
    }

    try {
      const files = casc.listStorageFiles(storageAddress, queryOptions);
      for (const entry of files) {
        if (results.has(entry.path)) {
          continue;
        }
        results.set(entry.path, entry);
        if (Number.isFinite(options.limit)) {
          remaining -= 1;
          if (remaining === 0) {
            break;
          }
        }
      }
    } catch (error) {
      logStageError('listStorageFiles', error, { ...options.context, activeMask });
      return null;
    }
  }

  return Array.from(results.values());
}

function emitJsonListing(
  casc: CascLib,
  files: CascStorageFileEntry[],
  options: { outputPath?: string },
): void {
  try {
    const tree = casc.buildTreeFromEntries(files);
    const jsonContent = `${JSON.stringify(tree, null, 2)}\n`;

    if (options.outputPath) {
      try {
        const outputDir = path.dirname(options.outputPath);
        mkdirSync(outputDir, { recursive: true });
        writeFileSync(options.outputPath, jsonContent, 'utf8');
        console.log('JSON file listing written to:', options.outputPath);
      } catch (writeError) {
        console.error('Failed to write JSON listing to file:', writeError);
        process.exitCode = 1;
      }
    } else {
      process.stdout.write(jsonContent);
    }
  } catch (error) {
    console.error('Failed to convert listing to JSON tree:', error);
    process.exitCode = 1;
  }
}

export function listStorageFiles({
  storage,
  mask: maskInput,
  limit: rawLimit,
  listFile: listFileInput,
  json: jsonOutput,
  output: outputInput,
}: ListStorageFilesCommandOptions = {}): void {
  const storagePath = resolveStoragePath(storage);
  if (!storagePath) {
    return;
  }
  const sanitizedMask = maskInput?.trim();
  const mask = sanitizedMask && sanitizedMask.length > 0 ? sanitizedMask : DEFAULT_MASK;
  const limitArg = normalizeLimitInput(rawLimit);
  const trimmedLimitArg = limitArg?.trim();
  const limit = parseLimit(trimmedLimitArg);
  const rawListFile = listFileInput?.trim();
  const listFile = rawListFile && rawListFile.length > 0 ? rawListFile : undefined;
  const shouldEmitJson = Boolean(jsonOutput);
  const trimmedOutput = outputInput?.trim();
  const outputPath = trimmedOutput && trimmedOutput.length > 0
    ? path.resolve(process.cwd(), trimmedOutput)
    : undefined;

  if (!shouldEmitJson && outputPath) {
    console.warn('Ignoring --output because --json was not specified.');
  }

  const errorContext: StageErrorContext = {
    storagePath,
    mask,
    maskInput: sanitizedMask && sanitizedMask.length > 0 ? sanitizedMask : undefined,
    limitValue: limit,
    limitInput: trimmedLimitArg && trimmedLimitArg.length > 0 ? trimmedLimitArg : undefined,
    listFile,
  };

  const casc = new CascLib(process.env.CASC_LIBRARY_PATH ?? path.resolve(__dirname, '../../../casc.framework/casc'));
  let storageAddress: bigint | null = null;

  try {
    const handle = openStorageHandle(casc, storagePath, errorContext);
    if (handle === null) {
      return;
    }
    storageAddress = handle;

    const info = fetchStorageInfo(casc, handle, errorContext);
    if (!info) {
      return;
    }

    printStorageInfo(info);

    const files = aggregateFileEntries(casc, handle, {
      mask,
      limit,
      listFile,
      context: errorContext,
    });
    if (files === null) {
      return;
    }

    if (shouldEmitJson) {
      emitJsonListing(casc, files, { outputPath });
    } else {
      printFileListing(files, mask, limit, listFile);
    }
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
