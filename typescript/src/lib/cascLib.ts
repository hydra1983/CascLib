import path from 'node:path';
import {
  open,
  define,
  DataType,
  createPointer,
  restorePointer,
  freePointer,
  PointerType,
} from 'ffi-rs';
import { cascErrorToString } from './cascErrors';

const ERROR_INSUFFICIENT_BUFFER = 0x7a;
const ERROR_ENOBUFS = 55;
const POINTER_SIZE = 8;
const ERROR_NO_MORE_FILES = 1001;
const ERROR_NO_MORE_FILES_WIN32 = 18;
const ERROR_FILE_NOT_FOUND = 2;
const ERROR_PATH_NOT_FOUND = 3;
const ERROR_HANDLE_EOF = 38;
// Match CASCLib's MAX_PATH (CascPort.h) which is 1024 on POSIX builds.
const MAX_PATH_LENGTH = 1024;
// sizeof(CASC_FIND_DATA) is roughly MAX_PATH + ~84 bytes; allocate extra headroom.
const CASC_FIND_DATA_SIZE = MAX_PATH_LENGTH + 512;
const MD5_HASH_SIZE = 16;
const TAG_MASK_BYTES = 8;
const FILE_SIZE_OFFSET = MAX_PATH_LENGTH + MD5_HASH_SIZE * 2 + TAG_MASK_BYTES;
const INVALID_HANDLE_VALUE = BigInt.asUintN(64, -1n);

function formatCascError(operation: string, errorCode: number, detail?: string): string {
  const prefix = detail ? `${operation} ${detail}` : operation;
  return `${prefix}: ${cascErrorToString(errorCode)} (${errorCode})`;
}

function readCString(buffer: Buffer, start: number, maxLength: number): string {
  const limit = Math.min(buffer.length, start + maxLength);
  for (let offset = start; offset < limit; offset++) {
    if (buffer[offset] === 0) {
      return buffer.toString('utf8', start, offset);
    }
  }
  return buffer.toString('utf8', start, limit);
}

function isInvalidHandle(handle: bigint): boolean {
  return handle === 0n || handle === -1n || handle === INVALID_HANDLE_VALUE;
}

export const CascStorageInfoClass = Object.freeze({
  LocalFileCount: 0,
  TotalFileCount: 1,
  Features: 2,
  Product: 4,
  Tags: 5,
  PathProduct: 6,
} as const);

export type CascStorageInfoClass = (typeof CascStorageInfoClass)[keyof typeof CascStorageInfoClass];

export interface CascTagInfo {
  name: string;
  length: number;
  value: number;
}

export interface CascProductInfo {
  codeName: string;
  buildNumber: number;
}

export interface CascFeatureInfo {
  value: number;
  features: string[];
}

export interface CascFileTreeNode {
  name: string;
  type: 'file' | 'directory';
  fullPath: string;
  children?: CascFileTreeNode[];
  size?: number;
}

export interface CascStorageFileEntry {
  path: string;
  size: number;
}

export type CascStorageInfoValue =
  | number
  | string
  | Buffer
  | CascProductInfo
  | CascFeatureInfo
  | CascTagInfo[];

export interface CascStorageInfo {
  localFileCount: number;
  totalFileCount: number;
  features: CascFeatureInfo;
  product: CascProductInfo;
  tags: CascTagInfo[];
  pathProduct: string;
}

function buildNativeBindings(libraryKey: string) {
  return define({
    CascOpenStorageEx: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.String, // LPCTSTR szParams
        DataType.External, // PCASC_OPEN_STORAGE_ARGS pArgs
        DataType.Boolean, // bool bOnlineStorage
        DataType.External, // HANDLE * phStorage
      ],
    },
    CascOpenStorage: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.String, // LPCTSTR szParams
        DataType.I32, // DWORD dwLocaleMask
        DataType.External, // HANDLE * phStorage
      ],
    },
    CascOpenOnlineStorage: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.String, // LPCTSTR szParams
        DataType.I32, // DWORD dwLocaleMask
        DataType.External, // HANDLE * phStorage
      ],
    },
    CascGetStorageInfo: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hStorage
        DataType.I32, // CASC_STORAGE_INFO_CLASS InfoClass
        DataType.U8Array, // void * pvStorageInfo
        DataType.BigInt, // size_t cbStorageInfo
        DataType.External, // size_t * pcbLengthNeeded
      ],
    },
    CascCloseStorage: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hStorage
      ],
    },
    CascOpenFile: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hStorage
        DataType.String, // const void * pvFileName
        DataType.I32, // DWORD dwLocaleFlags
        DataType.I32, // DWORD dwOpenFlags
        DataType.External, // HANDLE * PtrFileHandle
      ],
    },
    CascOpenLocalFile: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.String, // LPCTSTR szFileName
        DataType.I32, // DWORD dwOpenFlags
        DataType.External, // HANDLE * PtrFileHandle
      ],
    },
    CascGetFileInfo: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hFile
        DataType.I32, // CASC_FILE_INFO_CLASS InfoClass
        DataType.U8Array, // void * pvFileInfo
        DataType.BigInt, // size_t cbFileInfo
        DataType.External, // size_t * pcbLengthNeeded
      ],
    },
    CascSetFileFlags: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hFile
        DataType.I32, // DWORD dwOpenFlags
      ],
    },
    CascGetFileSize64: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hFile
        DataType.External, // PULONGLONG PtrFileSize
      ],
    },
    CascSetFilePointer64: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hFile
        DataType.BigInt, // LONGLONG DistanceToMove
        DataType.External, // PULONGLONG PtrNewPos
        DataType.I32, // DWORD dwMoveMethod
      ],
    },
    CascReadFile: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hFile
        DataType.U8Array, // void * lpBuffer
        DataType.I32, // DWORD dwToRead
        DataType.External, // PDWORD pdwRead
      ],
    },
    CascCloseFile: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hFile
      ],
    },
    CascGetFileSize: {
      library: libraryKey,
      retType: DataType.I32,
      paramsType: [
        DataType.BigInt, // HANDLE hFile
        DataType.External, // PDWORD pdwFileSizeHigh
      ],
    },
    CascSetFilePointer: {
      library: libraryKey,
      retType: DataType.I32,
      paramsType: [
        DataType.BigInt, // HANDLE hFile
        DataType.I32, // LONG lFilePos
        DataType.External, // LONG * PtrFilePosHigh
        DataType.I32, // DWORD dwMoveMethod
      ],
    },
    CascFindFirstFile: {
      library: libraryKey,
      retType: DataType.BigInt,
      paramsType: [
        DataType.BigInt, // HANDLE hStorage
        DataType.String, // LPCSTR szMask
        DataType.U8Array, // PCASC_FIND_DATA pFindData
        DataType.String, // LPCTSTR szListFile
      ],
    },
    CascFindNextFile: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hFind
        DataType.U8Array, // PCASC_FIND_DATA pFindData
      ],
    },
    CascFindClose: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hFind
      ],
    },
    CascAddEncryptionKey: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hStorage
        DataType.BigInt, // ULONGLONG KeyName
        DataType.U8Array, // LPBYTE Key
      ],
    },
    CascAddStringEncryptionKey: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hStorage
        DataType.BigInt, // ULONGLONG KeyName
        DataType.String, // LPCSTR szKey
      ],
    },
    CascImportKeysFromString: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hStorage
        DataType.String, // LPCSTR szKeyList
      ],
    },
    CascImportKeysFromFile: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hStorage
        DataType.String, // LPCTSTR szFileName
      ],
    },
    CascFindEncryptionKey: {
      library: libraryKey,
      retType: DataType.BigInt,
      paramsType: [
        DataType.BigInt, // HANDLE hStorage
        DataType.BigInt, // ULONGLONG KeyName
      ],
    },
    CascGetNotFoundEncryptionKey: {
      library: libraryKey,
      retType: DataType.Boolean,
      paramsType: [
        DataType.BigInt, // HANDLE hStorage
        DataType.External, // ULONGLONG * KeyName
      ],
    },
    CascCdnGetDefault: {
      library: libraryKey,
      retType: DataType.String,
      paramsType: [],
    },
    CascCdnDownload: {
      library: libraryKey,
      retType: DataType.BigInt,
      paramsType: [
        DataType.String, // LPCTSTR szCdnHostUrl
        DataType.String, // LPCTSTR szProduct
        DataType.String, // LPCTSTR szFileName
        DataType.External, // DWORD * PtrSize
      ],
    },
    CascCdnFree: {
      library: libraryKey,
      retType: DataType.Void,
      paramsType: [
        DataType.External, // void * buffer
      ],
    },
    SetCascError: {
      library: libraryKey,
      retType: DataType.Void,
      paramsType: [
        DataType.I32, // DWORD dwErrCode
      ],
    },
    GetCascError: {
      library: libraryKey,
      retType: DataType.I32,
      paramsType: [],
    },
  });
}

function parseStorageTags(buffer: Buffer): CascTagInfo[] {
  if (!Buffer.isBuffer(buffer) || buffer.length < 16) {
    return [];
  }

  const tagCount = Number(buffer.readBigUInt64LE(0));
  const tags: CascTagInfo[] = [];

  let entryOffset = 16;
  let nameOffset = entryOffset + tagCount * (POINTER_SIZE + 4 + 4);

  for (let i = 0; i < tagCount; i++) {
    const entryStart = entryOffset + i * (POINTER_SIZE + 4 + 4);
    if (entryStart + POINTER_SIZE + 8 > buffer.length) {
      break;
    }

    const nameLength = buffer.readUInt32LE(entryStart + POINTER_SIZE);
    const tagValue = buffer.readUInt32LE(entryStart + POINTER_SIZE + 4);

    const nameEnd = nameOffset + nameLength;
    if (nameEnd > buffer.length) {
      break;
    }

    const name = buffer.toString('utf8', nameOffset, nameEnd).replace(/\0+$/, '');
    nameOffset = nameEnd + 1;

    tags.push({
      name,
      length: nameLength,
      value: tagValue,
    });
  }

  return tags;
}

function parseFeatureFlags(value: number): string[] {
  const flagMap = [
    { bit: 0x00000001, name: 'FILE_NAMES' },
    { bit: 0x00000002, name: 'ROOT_CKEY' },
    { bit: 0x00000004, name: 'TAGS' },
    { bit: 0x00000008, name: 'FNAME_HASHES' },
    { bit: 0x00000010, name: 'FNAME_HASHES_OPTIONAL' },
    { bit: 0x00000020, name: 'FILE_DATA_IDS' },
    { bit: 0x00000040, name: 'LOCALE_FLAGS' },
    { bit: 0x00000080, name: 'CONTENT_FLAGS' },
    { bit: 0x00000100, name: 'DATA_ARCHIVES' },
    { bit: 0x00000200, name: 'DATA_FILES' },
    { bit: 0x00000400, name: 'ONLINE' },
    { bit: 0x00001000, name: 'FORCE_DOWNLOAD' },
  ];

  return flagMap.filter(({ bit }) => (value & bit) !== 0).map(({ name }) => name);
}

export class CascLib {
  private readonly libraryKey: string;
  private readonly libraryPath: string;
  private readonly native: ReturnType<typeof buildNativeBindings>;

  constructor(libraryPath: string, libraryKey = 'casc-lib') {
    this.libraryKey = libraryKey;
    this.libraryPath = path.isAbsolute(libraryPath)
      ? libraryPath
      : path.resolve(__dirname, libraryPath);

    open({ library: this.libraryKey, path: this.libraryPath });
    this.native = buildNativeBindings(this.libraryKey);
  }

  openStorage(storagePath: string): bigint {
    const pointer = createPointer({ paramsType: [DataType.BigInt], paramsValue: [0n] });

    const opened = this.native.CascOpenStorage([
      storagePath,
      0,
      pointer[0],
    ]);

    if (!opened) {
      freePointer({ paramsType: [DataType.BigInt], paramsValue: pointer, pointerType: PointerType.RsPointer });
      const errorCode = this.getLastError();
      throw new Error(formatCascError('CascOpenStorage', errorCode, 'failed'));
    }

    const address = restorePointer({ retType: [DataType.BigInt], paramsValue: pointer })[0];
    freePointer({ paramsType: [DataType.BigInt], paramsValue: pointer, pointerType: PointerType.RsPointer });

    if (address === 0n) {
      const errorCode = this.getLastError();
      throw new Error(formatCascError('CascOpenStorage', errorCode, 'returned null handle'));
    }

    return address;
  }

  getStorageInfo(handle: bigint | number, infoClass: CascStorageInfoClass): CascStorageInfoValue {
    const storageAddress = typeof handle === 'bigint' ? handle : BigInt(handle);

    const sizePointer = createPointer({ paramsType: [DataType.BigInt], paramsValue: [0n] });

    this.native.CascGetStorageInfo([
      storageAddress,
      infoClass,
      new Uint8Array(0),
      0n,
      sizePointer[0],
    ]);

    const requiredSize = Number(restorePointer({ retType: [DataType.BigInt], paramsValue: sizePointer })[0]);
    freePointer({ paramsType: [DataType.BigInt], paramsValue: sizePointer, pointerType: PointerType.RsPointer });

    if (!Number.isFinite(requiredSize) || requiredSize <= 0) {
      const errorCode = this.getLastError();
      throw new Error(formatCascError('CascGetStorageInfo', errorCode, 'size query failed'));
    }

    const buffer = Buffer.alloc(requiredSize);
    const resultPointer = createPointer({ paramsType: [DataType.BigInt], paramsValue: [BigInt(buffer.length)] });

    const success = this.native.CascGetStorageInfo([
      storageAddress,
      infoClass,
      new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.length),
      BigInt(buffer.length),
      resultPointer[0],
    ]);

    const written = Number(restorePointer({ retType: [DataType.BigInt], paramsValue: resultPointer })[0]);
    freePointer({ paramsType: [DataType.BigInt], paramsValue: resultPointer, pointerType: PointerType.RsPointer });

    if (!success) {
      const errorCode = this.getLastError();
      throw new Error(formatCascError('CascGetStorageInfo', errorCode, 'failed'));
    }

    const effectiveBuffer = Number.isFinite(written) && written > 0
      ? buffer.slice(0, Math.min(written, buffer.length))
      : buffer;

    switch (infoClass) {
      case CascStorageInfoClass.LocalFileCount:
      case CascStorageInfoClass.TotalFileCount:
        return effectiveBuffer.length >= 4 ? effectiveBuffer.readUInt32LE(0) : 0;

      case CascStorageInfoClass.Features: {
        const value = effectiveBuffer.length >= 4 ? effectiveBuffer.readUInt32LE(0) : 0;
        return {
          value,
          features: parseFeatureFlags(value),
        } satisfies CascFeatureInfo;
      }

      case CascStorageInfoClass.Product: {
        const zeroIndex = effectiveBuffer.indexOf(0);
        const codeNameEnd = zeroIndex >= 0 ? zeroIndex : 0x1c;
        const codeName = effectiveBuffer.toString('utf8', 0, codeNameEnd);
        const buildNumber = effectiveBuffer.length >= 0x20 ? effectiveBuffer.readUInt32LE(0x1c) : 0;
        return { codeName, buildNumber } satisfies CascProductInfo;
      }

      case CascStorageInfoClass.Tags:
        return parseStorageTags(effectiveBuffer);

      case CascStorageInfoClass.PathProduct: {
        const zeroIndex = effectiveBuffer.indexOf(0);
        const end = zeroIndex >= 0 ? zeroIndex : undefined;
        return effectiveBuffer.toString('utf8', 0, end);
      }

      default:
        return effectiveBuffer;
    }
  }

  private buildFileTreeFromEntries(entries: CascStorageFileEntry[]): CascFileTreeNode {
    type TreeNode = CascFileTreeNode & {
      children: TreeNode[];
      childMap?: Map<string, TreeNode>;
    };

    const ensureDirectory = (parent: TreeNode, name: string, fullPath: string): TreeNode => {
      parent.childMap ??= new Map();
      const existing = parent.childMap.get(name);
      if (existing && existing.type === 'directory') {
        return existing;
      }

      const node: TreeNode = {
        name,
        type: 'directory',
        fullPath,
        children: [],
        childMap: new Map(),
      };
      parent.children.push(node);
      parent.childMap.set(name, node);
      return node;
    };

    const addFile = (parent: TreeNode, name: string, fullPath: string, size: number): void => {
      parent.childMap ??= new Map();
      if (parent.childMap.has(name)) {
        return;
      }

      const node: TreeNode = {
        name,
        type: 'file',
        fullPath,
        size,
        children: [],
      };
      parent.children.push(node);
      parent.childMap.set(name, node);
    };

    const root: TreeNode = {
      name: '',
      type: 'directory',
      fullPath: '',
      children: [],
      childMap: new Map(),
    };

    const normalizePathEntry = (entry: string): string | null => {
      const trimmed = entry.trim();
      if (trimmed.length === 0) {
        return null;
      }

      if (trimmed === 'vfs-root') {
        return null;
      }

      if (trimmed.endsWith(':')) {
        return null;
      }

      const colonNormalized = trimmed.replace(/:/g, '/');
      const slashNormalized = colonNormalized.replace(/[\\/]+/g, '/');
      const sanitized = slashNormalized.replace(/^\/+|\/+$/g, '');
      if (sanitized.length === 0) {
        return null;
      }

      return sanitized.startsWith('vfs-root') ? sanitized : `vfs-root/${sanitized}`;
    };

    for (const entry of entries) {
      const normalizedPath = normalizePathEntry(entry.path);
      if (!normalizedPath) {
        continue;
      }

      const segments = normalizedPath.split('/').filter(segment => segment.length > 0);
      if (segments.length === 0) {
        continue;
      }

      let current = root;
      let currentPath = '';

      for (let index = 0; index < segments.length; index += 1) {
        const segment = segments[index];
        currentPath = currentPath.length === 0 ? segment : `${currentPath}/${segment}`;
        const isLeaf = index === segments.length - 1;

        if (isLeaf) {
          addFile(current, segment, currentPath, entry.size);
        } else {
          current = ensureDirectory(current, segment, currentPath);
        }
      }
    }

    const convertNode = (node: TreeNode): CascFileTreeNode => {
      let children: CascFileTreeNode[] | undefined;

      if (node.type === 'directory') {
        const convertedChildren: CascFileTreeNode[] = [];
        for (const child of node.children) {
          convertedChildren.push(convertNode(child));
        }

        convertedChildren.sort((a, b) => {
          if (a.type !== b.type) {
            return a.type === 'directory' ? -1 : 1;
          }
          return a.name.localeCompare(b.name);
        });

        if (convertedChildren.length > 0) {
          children = convertedChildren;
        }
      }

      if (node.childMap) {
        node.childMap.clear();
        delete node.childMap;
      }

      const result: CascFileTreeNode = {
        name: node.name,
        type: node.type,
        fullPath: node.fullPath,
      };

      if (node.type === 'file' && typeof node.size === 'number') {
        result.size = node.size;
      }

      if (children && children.length > 0) {
        result.children = children;
      }

      return result;
    };

    const tree = convertNode(root);

    const rebaseFullPath = (node: CascFileTreeNode, prefix: string): void => {
      if (node.fullPath === prefix) {
        node.fullPath = '';
      } else if (node.fullPath.startsWith(`${prefix}/`)) {
        node.fullPath = node.fullPath.slice(prefix.length + 1);
      }

      if (node.children) {
        node.children.forEach(child => rebaseFullPath(child, prefix));
      }
    };

    if (tree.children) {
      const vfsRoot = tree.children.find(node => node.name === 'vfs-root' && node.type === 'directory');
      if (vfsRoot) {
        rebaseFullPath(vfsRoot, 'vfs-root');
        return vfsRoot;
      }
    }

    return tree;
  }

  listStorageFiles(
    handle: bigint | number,
    options: { limit?: number; mask?: string; listFile?: string } = {},
  ): CascStorageFileEntry[] {
    const storageAddress = typeof handle === 'bigint' ? handle : BigInt(handle);
    const maxCount = options.limit === undefined
      ? Infinity
      : Math.max(0, Math.floor(options.limit));

    if (maxCount === 0) {
      return [];
    }

    const findDataBuffer = Buffer.alloc(CASC_FIND_DATA_SIZE);
    const findDataView = new Uint8Array(findDataBuffer.buffer, findDataBuffer.byteOffset, findDataBuffer.length);
    const mask = options.mask ?? '*';
    const listFile = options.listFile ?? '';

    const readFileSize = (): number => {
      if (FILE_SIZE_OFFSET + 8 > findDataBuffer.length) {
        return 0;
      }

      try {
        return Number(findDataBuffer.readBigUInt64LE(FILE_SIZE_OFFSET));
      } catch (error) {
        // Node <12.0 does not support readBigUInt64LE; fall back to manual parsing
        const low = findDataBuffer.readUInt32LE(FILE_SIZE_OFFSET);
        const high = findDataBuffer.readUInt32LE(FILE_SIZE_OFFSET + 4);
        return high * 0x100000000 + low;
      }
    };

    this.native.SetCascError([0]); // Avoid stale errors if the search yields no matches
    const findHandle = this.native.CascFindFirstFile([
      storageAddress,
      mask,
      findDataView,
      listFile,
    ]) as bigint;

    if (isInvalidHandle(findHandle)) {
      const errorCode = this.getLastError();
      if (
        errorCode === ERROR_NO_MORE_FILES
        || errorCode === ERROR_NO_MORE_FILES_WIN32
        || errorCode === ERROR_FILE_NOT_FOUND
        || errorCode === ERROR_PATH_NOT_FOUND
        || errorCode === 0
      ) {
        return [];
      }

      throw new Error(formatCascError('CascFindFirstFile', errorCode, 'failed'));
    }

    const entries: CascStorageFileEntry[] = [];
    let iterationFailed = false;

    const pushCurrentEntry = () => {
      const entryName = readCString(findDataBuffer, 0, MAX_PATH_LENGTH);
      if (entryName.length === 0) {
        return;
      }

      entries.push({ path: entryName, size: readFileSize() });
    };

    try {
      pushCurrentEntry();

      while (entries.length < maxCount) {
        const success = this.native.CascFindNextFile([
          findHandle,
          findDataView,
        ]);

        if (!success) {
          const errorCode = this.getLastError();
          if (
            errorCode === ERROR_NO_MORE_FILES
            || errorCode === ERROR_NO_MORE_FILES_WIN32
            || errorCode === ERROR_FILE_NOT_FOUND
            || errorCode === ERROR_PATH_NOT_FOUND
          ) {
            break;
          }
          throw new Error(formatCascError('CascFindNextFile', errorCode, 'failed'));
        }

        pushCurrentEntry();
      }
    } catch (error) {
      iterationFailed = true;
      throw error;
    } finally {
      const closed = this.native.CascFindClose([findHandle]);
      if (!closed && !iterationFailed) {
        const errorCode = this.getLastError();
        throw new Error(formatCascError('CascFindClose', errorCode, 'failed'));
      }
    }

    return entries;
  }

  buildTreeFromEntries(entries: CascStorageFileEntry[]): CascFileTreeNode {
    return this.buildFileTreeFromEntries(entries);
  }

  openFile(
    storageHandle: bigint | number,
    filePath: string,
    localeFlags = 0,
    openFlags = 0,
  ): bigint {
    const storageAddress = typeof storageHandle === 'bigint' ? storageHandle : BigInt(storageHandle);
    const handlePointer = createPointer({ paramsType: [DataType.BigInt], paramsValue: [0n] });

    const opened = this.native.CascOpenFile([
      storageAddress,
      filePath,
      localeFlags,
      openFlags,
      handlePointer[0],
    ]);

    const handle = restorePointer({ retType: [DataType.BigInt], paramsValue: handlePointer })[0];
    freePointer({ paramsType: [DataType.BigInt], paramsValue: handlePointer, pointerType: PointerType.RsPointer });

    if (!opened || isInvalidHandle(handle)) {
      const errorCode = this.getLastError();
      throw new Error(formatCascError('CascOpenFile', errorCode, `failed for "${filePath}"`));
    }

    return handle;
  }

  closeFile(fileHandle: bigint | number): void {
    const cascHandle = typeof fileHandle === 'bigint' ? fileHandle : BigInt(fileHandle);
    const closed = this.native.CascCloseFile([cascHandle]);
    if (!closed) {
      const errorCode = this.getLastError();
      throw new Error(formatCascError('CascCloseFile', errorCode, 'failed'));
    }
  }

  getFileSize(handle: bigint | number): bigint {
    const cascHandle = typeof handle === 'bigint' ? handle : BigInt(handle);
    const sizePointer = createPointer({ paramsType: [DataType.BigInt], paramsValue: [0n] });
    const success = this.native.CascGetFileSize64([
      cascHandle,
      sizePointer[0],
    ]);

    const size = restorePointer({ retType: [DataType.BigInt], paramsValue: sizePointer })[0];
    freePointer({ paramsType: [DataType.BigInt], paramsValue: sizePointer, pointerType: PointerType.RsPointer });

    if (!success) {
      const errorCode = this.getLastError();
      throw new Error(formatCascError('CascGetFileSize64', errorCode, 'failed'));
    }

    return size;
  }

  readFile(handle: bigint | number, buffer: Buffer, length?: number): number {
    const cascHandle = typeof handle === 'bigint' ? handle : BigInt(handle);
    const bytesToRead = length === undefined ? buffer.length : Math.min(buffer.length, length);
    const readPointer = createPointer({ paramsType: [DataType.I32], paramsValue: [0] });

    const success = this.native.CascReadFile([
      cascHandle,
      new Uint8Array(buffer.buffer, buffer.byteOffset, bytesToRead),
      bytesToRead,
      readPointer[0],
    ]);

    const bytesRead = restorePointer({ retType: [DataType.I32], paramsValue: readPointer })[0];
    freePointer({ paramsType: [DataType.I32], paramsValue: readPointer, pointerType: PointerType.RsPointer });

    if (!success && bytesRead === 0) {
      const errorCode = this.getLastError();
      if (errorCode !== ERROR_HANDLE_EOF) {
        throw new Error(formatCascError('CascReadFile', errorCode, 'failed'));
      }
    }

    return bytesRead;
  }

  buildStorageFileTree(
    handle: bigint | number,
    options: { limit?: number; mask?: string; listFile?: string } = {},
  ): CascFileTreeNode {
    const entries = this.listStorageFiles(handle, options);
    return this.buildFileTreeFromEntries(entries);
  }

  closeStorage(handle: bigint | number): void {
    const storageAddress = typeof handle === 'bigint' ? handle : BigInt(handle);
    const closed = this.native.CascCloseStorage([storageAddress]);
    if (!closed) {
      const errorCode = this.getLastError();
      throw new Error(formatCascError('CascCloseStorage', errorCode, 'failed'));
    }
  }

  getLastError(): number {
    return this.native.GetCascError([]);
  }

  getStorage(handle: bigint | number): CascStorageInfo {
    const localFileCount = this.getStorageInfo(handle, CascStorageInfoClass.LocalFileCount) as number;
    const totalFileCount = this.getStorageInfo(handle, CascStorageInfoClass.TotalFileCount) as number;
    const features = this.getStorageInfo(handle, CascStorageInfoClass.Features) as CascFeatureInfo;
    const product = this.getStorageInfo(handle, CascStorageInfoClass.Product) as CascProductInfo;
    const tags = this.getStorageInfo(handle, CascStorageInfoClass.Tags) as CascTagInfo[];
    const pathProduct = this.getStorageInfo(handle, CascStorageInfoClass.PathProduct) as string;

    return {
      localFileCount,
      totalFileCount,
      features,
      product,
      tags,
      pathProduct,
    } satisfies CascStorageInfo;
  }
}
