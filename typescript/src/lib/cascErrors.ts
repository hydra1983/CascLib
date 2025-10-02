/**
 * Human-readable error descriptions for CascLib error codes.
 *
 * Win32 error names are sourced from the mapping in `CascPort.h` where, on
 * non-Windows platforms, they are redefined in terms of standard `errno`
 * values.  The CascLib-specific errors (>= 0x20000000) are also declared in
 * `CascPort.h`.
 */
import { constants as osConstants } from 'node:os';

const errno = osConstants?.errno ?? {};

function register(map: Map<number, string[]>, code: number | undefined, name: string): void {
  if (code === undefined || Number.isNaN(code)) return;
  const entry = map.get(code);
  if (entry) {
    if (!entry.includes(name)) entry.push(name);
  } else {
    map.set(code, [name]);
  }
}

const ERROR_MAP = new Map<number, string[]>();

// Generic / mapped Win32 error codes
register(ERROR_MAP, 0, 'ERROR_SUCCESS');
register(ERROR_MAP, errno.ENOENT ?? 2, 'ERROR_FILE_NOT_FOUND');
register(ERROR_MAP, errno.ENOENT ?? 2, 'ERROR_PATH_NOT_FOUND');
register(ERROR_MAP, errno.EPERM ?? 1, 'ERROR_ACCESS_DENIED');
register(ERROR_MAP, errno.EBADF ?? 9, 'ERROR_INVALID_HANDLE');
register(ERROR_MAP, errno.ENOMEM ?? 12, 'ERROR_NOT_ENOUGH_MEMORY');
register(ERROR_MAP, errno.ENOTSUP ?? 95, 'ERROR_NOT_SUPPORTED');
register(ERROR_MAP, errno.EINVAL ?? 22, 'ERROR_INVALID_PARAMETER');
register(ERROR_MAP, errno.ENOSPC ?? 28, 'ERROR_DISK_FULL');
register(ERROR_MAP, errno.EEXIST ?? 17, 'ERROR_ALREADY_EXISTS');
register(ERROR_MAP, errno.ENOBUFS ?? 105, 'ERROR_INSUFFICIENT_BUFFER');

// CascLib custom error codes (see CascPort.h)
register(ERROR_MAP, 1000, 'ERROR_BAD_FORMAT');
register(ERROR_MAP, 1001, 'ERROR_NO_MORE_FILES');
register(ERROR_MAP, 1002, 'ERROR_HANDLE_EOF');
register(ERROR_MAP, 1003, 'ERROR_CAN_NOT_COMPLETE');
register(ERROR_MAP, 1004, 'ERROR_FILE_CORRUPT');
register(ERROR_MAP, 1005, 'ERROR_FILE_ENCRYPTED');
register(ERROR_MAP, 1006, 'ERROR_FILE_TOO_LARGE');
register(ERROR_MAP, 1006, 'ERROR_FILE_INCOMPLETE');
register(ERROR_MAP, 1007, 'ERROR_ARITHMETIC_OVERFLOW');
register(ERROR_MAP, 1007, 'ERROR_FILE_OFFLINE');
register(ERROR_MAP, 1008, 'ERROR_NETWORK_NOT_AVAILABLE');
register(ERROR_MAP, 1008, 'ERROR_BUFFER_OVERFLOW');
register(ERROR_MAP, 1009, 'ERROR_CANCELLED');
register(ERROR_MAP, 1010, 'ERROR_INDEX_PARSING_DONE');
register(ERROR_MAP, 1011, 'ERROR_REPARSE_ROOT');
register(ERROR_MAP, 1012, 'ERROR_CKEY_ALREADY_OPENED');

// CascLib extended errors (0x2000_0000 block)
register(ERROR_MAP, 0x20000000, 'ERROR_CASC_INTERNAL');
register(ERROR_MAP, 0x20000001, 'ERROR_CASC_DIRECTORY_INVALID');
register(ERROR_MAP, 0x20000002, 'ERROR_CASC_INVALID_IMPORT');
register(ERROR_MAP, 0x20000003, 'ERROR_CASC_DATABASE_FULL');
register(ERROR_MAP, 0x20000004, 'ERROR_CASC_STARVING_CACHE');
register(ERROR_MAP, 0x20000005, 'ERROR_CASC_HASH_TABLE');
register(ERROR_MAP, 0x20000006, 'ERROR_CASC_HASH_ENTRY');
register(ERROR_MAP, 0x20000007, 'ERROR_CASC_INVALID_OFFS');
register(ERROR_MAP, 0x20000008, 'ERROR_CASC_INVALID_INDEX');
register(ERROR_MAP, 0x20000009, 'ERROR_CASC_INDEX_FILES');
register(ERROR_MAP, 0x2000000A, 'ERROR_CASC_INVALID_ENCODING');
register(ERROR_MAP, 0x2000000B, 'ERROR_CASC_KEY_MISSING');
register(ERROR_MAP, 0x2000000C, 'ERROR_CASC_DATA_FILE');
register(ERROR_MAP, 0x2000000D, 'ERROR_CASC_TOO_MANY_OPEN_FILES');
register(ERROR_MAP, 0x2000000E, 'ERROR_CASC_NOT_IMPLEMENTED');
register(ERROR_MAP, 0x2000000F, 'ERROR_CASC_CACHE_INDEX');
register(ERROR_MAP, 0x20000010, 'ERROR_CASC_MISSING_LISTFILE');
register(ERROR_MAP, 0x20000011, 'ERROR_CASC_MIXED_ENCODING');
register(ERROR_MAP, 0x20000012, 'ERROR_CASC_UNKNOWN_FILE_KEY');
register(ERROR_MAP, 0x20000013, 'ERROR_CASC_CLOUD_SYNC_ENABLED');

export function cascErrorToString(code: number): string {
  const names = ERROR_MAP.get(code);
  if (!names || names.length === 0) {
    return `UNKNOWN_ERROR (${code})`;
  }
  return names.join(' | ');
}
