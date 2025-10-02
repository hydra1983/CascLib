import {
  listStorageFiles,
  type ListStorageFilesCommandOptions,
} from './commands/list';
import {
  extractStorageEntries,
  type ExtractStorageEntriesCommandOptions,
} from './commands/extract';

type CommonCommandOptions = Pick<ListStorageFilesCommandOptions, 'storage' | 'mask' | 'limit' | 'listFile'>;

const DEFAULT_COMMAND_USAGE = '<storage>';

const withStoragePositional = (cli: any): any =>
  cli.positional('storage', {
    describe: 'Path to the CASC storage root directory',
    type: 'string',
    demandOption: true,
  });

const withCommonOptions = (cli: any): any =>
  withStoragePositional(cli)
    .option('mask', {
      alias: 'm',
      describe: 'Glob mask used when querying the storage',
      type: 'string',
    })
    .option('limit', {
      alias: 'l',
      describe: 'Maximum number of entries to process, or "all"',
      type: 'string',
    })
    .option('list-file', {
      alias: ['f'],
      describe: 'Optional casc listfile to limit enumeration',
      type: 'string',
    });

async function run(argv: string[]): Promise<void> {
  const dynamicImport = new Function(
    'specifier',
    'return import(specifier);',
  ) as (specifier: string) => Promise<any>;

  const { default: yargs } = await dynamicImport('yargs');
  const parser = yargs(argv.slice(2));

  parser
    .scriptName('casc')
    .usage('$0 <command>')
    .command({
      command: `list ${DEFAULT_COMMAND_USAGE}`,
      aliases: ['listStorageFiles', 'list-storage-files'],
      describe: 'Enumerate CASC storage entries with mask/listfile filters and optional JSON output',
      builder: (cli: any) => {
        const command = withCommonOptions(cli)
          .option('json', {
            describe: 'Emit the file listing as a JSON tree instead of text',
            type: 'boolean',
            default: false,
          })
          .option('output', {
            alias: ['o'],
            describe: 'Write the JSON output to a file instead of stdout',
            type: 'string',
          });

        command.example(
          '$0 list "/path/to/storage" --mask "data:*" --list-file fixtures/listfile.txt --limit 20',
          'List the first 20 entries that match the data: prefix via an external listfile',
        );
        command.example(
          '$0 list "/path/to/storage" --json --output output/storageFiles.json',
          'Emit the filtered results as a JSON tree and save them to output/storageFiles.json',
        );

        return command;
      },
      handler: (args: unknown) => {
        const { storage, mask, limit, listFile, json, output } = args as ListStorageFilesCommandOptions;
        listStorageFiles({ storage, mask, limit, listFile, json, output });
      },
    })
    .command({
      command: `extract ${DEFAULT_COMMAND_USAGE}`,
      aliases: ['extractStorageFiles', 'extract-storage-files'],
      describe: 'Copy matched CASC files to a local directory, with optional overwrite and chunk sizing',
      builder: (cli: any) => {
        const command = withCommonOptions(cli)
          .option('output', {
            alias: ['o'],
            describe: 'Destination directory for the extracted files',
            type: 'string',
          })
          .option('overwrite', {
            alias: 'w',
            describe: 'Overwrite existing files in the destination directory',
            type: 'boolean',
            default: false,
          })
          .option('chunk-size', {
            alias: 'c',
            describe: 'Size of each read chunk in bytes while streaming files',
            type: 'string',
          });

        command.example(
          '$0 extract "/path/to/storage" --mask "data:data\\local\\lng\\*\\*tbl" --output output/lng',
          'Extract localisation tables that match the mask into output/lng',
        );
        command.example(
          '$0 extract "/path/to/storage" --list-file fixtures/listfile.txt --limit 50 --output extracted --overwrite',
          'Use a listfile to expand matches, copy the first 50 files, and overwrite if they exist',
        );

        return command;
      },
      handler: (args: unknown) => {
        const {
          storage,
          mask,
          limit,
          listFile,
          output,
          overwrite,
          chunkSize,
        } = args as ExtractStorageEntriesCommandOptions;
        extractStorageEntries({
          storage,
          mask,
          limit,
          listFile,
          output,
          overwrite,
          chunkSize,
        });
      },
    })
    .demandCommand(1, 'Please specify a subcommand. Run with --help to see available options.')
    .strict()
    .help()
    .alias('h', 'help')
    .wrap(parser.terminalWidth())
    .parse();
}

void run(process.argv).catch(error => {
  console.error('Failed to run casc CLI:', error);
  process.exitCode = 1;
});
