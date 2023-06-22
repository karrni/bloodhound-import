import fs from 'fs';
import path from 'path';

import AdmZip from 'adm-zip';
import neo4j from 'neo4j-driver';
import sanitize from 'sanitize-filename';
import chain from 'stream-chain';
import parser from 'stream-json';
import pick from 'stream-json/filters/Pick.js';
import StreamArray from 'stream-json/streamers/StreamArray.js';
import batch from 'stream-json/utils/Batch.js';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';


// --- Ingestion Logic ---

// https://github.com/BloodHoundAD/BloodHound/blob/master/src/js/newingestion.js
import * as NewIngestion from './newingestion.js';

// https://github.com/BloodHoundAD/BloodHound/blob/master/src/components/Menu/MenuContainer.jsx#L32
const IngestFuncMap = {
    computers: NewIngestion.buildComputerJsonNew,
    groups: NewIngestion.buildGroupJsonNew,
    users: NewIngestion.buildUserJsonNew,
    domains: NewIngestion.buildDomainJsonNew,
    ous: NewIngestion.buildOuJsonNew,
    gpos: NewIngestion.buildGpoJsonNew,
    containers: NewIngestion.buildContainerJsonNew,
    azure: NewIngestion.convertAzureData,
};


// --- General ---

// Used within newingestion.js

// Add format() and formatn() functions to String objects
// https://github.com/BloodHoundAD/BloodHound/blob/master/src/index.js#L37
String.prototype.format = function () {
    let i = 0;
    const args = arguments;
    return this.replace(/{}/g, function () {
        return typeof args[i] !== 'undefined' ? args[i++] : '';
    });
};

String.prototype.formatn = function () {
    let formatted = this;
    for (let i = 0; i < arguments.length; i++) {
        const regexp = new RegExp('\\{' + i + '\\}', 'gi');
        formatted = formatted.replace(regexp, arguments[i]);
    }
    return formatted;
};

// Add chunk() function to Array objects
// https://github.com/BloodHoundAD/BloodHound/blob/master/src/index.js#L72
Array.prototype.chunk = function (chunkSize = 10000) {
    let i;
    let len = this.length;
    let temp = [];

    for (i = 0; i < len; i += chunkSize) {
        temp.push(this.slice(i, i + chunkSize));
    }

    return temp;
};


// --- Arguments ---

const args = yargs(hideBin(process.argv))
    .command(
        '$0 <file>',
        'import the given file',
        (yargs) => {
            yargs.positional('file', {
                describe: 'path to the file',
                type: 'string'
            })
        },
        (argv) => {
            processFile(argv.file).then(() => {
                console.log("All done :)");
                driver.close();
                return 0;
            });
        }
    )
    .option('a', {
        alias: 'address',
        description: 'Hostname/IP of the Neo4j server',
        type: 'string',
        demandOption: true,
    })
    .option('u', {
        alias: 'username',
        description: 'User for the Neo4j database',
        type: 'string',
        demandOption: true,
    })
    .option('p', {
        alias: 'password',
        description: 'Password for the Neo4j database',
        type: 'string',
        demandOption: true,
    })
    .argv;


// --- Neo4j Database ---

const driver = neo4j.driver(
    `bolt://${args.address}:7687`,
    neo4j.auth.basic(
        args.username,
        args.password
    )
)

// "Upload" data to the Neo4j by executing the Cypher query
// https://github.com/BloodHoundAD/BloodHound/blob/master/src/components/Menu/MenuContainer.jsx#L357
async function uploadData(statement, props) {

    console.log("Uploading batch:", props.length);

    let session = driver.session();

    await session.run(
        statement,
        { props: props }
    ).catch((err) => {
        console.error("Error running statement:", statement);
        console.error(statement);
    });

    await session.close();
}

// Process the data and upload it to the Neo4j database
//     I don't entirely understand why azure data is treated differently,
//     but this is the exact procedure BloodHound uses.
// https://github.com/BloodHoundAD/BloodHound/blob/master/src/components/Menu/MenuContainer.jsx#L271
async function processAndUploadData(processedData, meta) {
    if (meta.type === 'azure') {
        for (let value of Object.values(
            processedData.AzurePropertyMaps
        )) {
            let props = value.Props;
            if (props.length === 0) continue;
            let chunked = props.chunk();
            let statement = value.Statement;

            for (let chunk of chunked) {
                await uploadData(statement, chunk);
            }
        }

        for (let item of Object.values(
            processedData.OnPremPropertyMaps
        )) {
            let props = item.Props;
            if (props.length === 0) continue;
            let chunked = props.chunk();
            let statement = item.Statement;

            for (let chunk of chunked) {
                await uploadData(statement, chunk);
            }
        }

        for (let item of Object.values(
            processedData.RelPropertyMaps
        )) {
            let props = item.Props;
            if (props.length === 0) continue;
            let chunked = props.chunk();
            let statement = item.Statement;

            for (let chunk of chunked) {
                await uploadData(statement, chunk);
            }
        }
    } else {
        for (let key in processedData) {
            let props = processedData[key].props;
            if (props.length === 0) continue;
            let chunked = props.chunk();
            let statement = processedData[key].statement;

            for (let chunk of chunked) {
                await uploadData(statement, chunk);
            }
        }
    }
}


// --- File Processing ---

async function processJsonFile(file) {
    console.log("Processing file:", file);

    let meta = await getMetaTagQuick(file);
    let processor = IngestFuncMap[meta.type];

    console.log("Data Type:", meta.type);
    console.log("Data Count:", meta.count);

    // Create a pipeline that processes the JSON file
    const processingPipeline = new chain([
        // Create a input stream for the file
        fs.createReadStream(file),
        // Parse the stream as JSON
        parser(),
        // Pick out the 'data' key
        new pick({ filter: 'data' }),
        // Create an array from the stream
        new StreamArray(),
        // Take the value of the item
        (data) => data.value,
        // Batch the items up in groups of 200
        new batch({ batchSize: 200 })
    ]);

    for await (const data of processingPipeline) {
        const processedData = processor(data);
        await processAndUploadData(processedData, meta);

    }

    console.log("Finished processing");
}

async function processFile(file) {
    const extension = path.extname(file);

    if (extension === '.zip') {
        console.log("Extracting archive:", file)
        const zip = new AdmZip(file);
        const entries = zip.getEntries();

        const tmpPath = path.join(process.cwd(), 'tmp');
        if (!fs.existsSync(tmpPath)) {
            fs.mkdirSync(tmpPath);
        }

        for (const entry of entries) {
            let entryName = sanitize(entry.entryName);
            let entryPath = path.join(tmpPath, entryName);

            zip.extractEntryTo(
                entry.entryName,
                tmpPath,
                false,
                true,
                false,
                entryName
            );

            await processJsonFile(entryPath);
            fs.unlinkSync(entryPath);
        }
    } else if (extension === '.json') {
        await processJsonFile(file);
    } else {
        console.error("Invalid input file, needs to be .zip or .json")
        return 1;
    }
}


// --- Parsing Data ---

// Quickly get the metadata from a BloodHound JSON file
// https://github.com/BloodHoundAD/BloodHound/blob/master/src/components/Menu/MenuContainer.jsx#L130
async function getMetaTagQuick(filePath) {
    let size = fs.statSync(filePath).size;
    let start = size - 300;

    if (start <= 0) {
        start = 0;
    }

    // Try reading the 'meta' key from the last 300 bytes of the file
    let meta = await readMetaFromChunk(filePath, { start: start, end: size });
    if (meta.type !== null && meta.count !== null) {
        return meta;
    }

    // Try reading the 'meta' key from the first 300 bytes of the file
    meta = await readMetaFromChunk(filePath, { start: 0, end: 300 });
    return meta;
}

// Function to read a chunk from the file and try to parse the metadata from it
async function readMetaFromChunk(filePath, { start, end }) {
    return new Promise((resolve, reject) => {
        fs.createReadStream(filePath, {
            encoding: 'utf8',
            start: start,
            end: end,
        }).on('data', (chunk) => {
            let type, version, count;

            try {
                type = /type.?:\s?"(\w*)"/g.exec(chunk)[1];
                count = parseInt(/"count.?:\s?(\d*)/g.exec(chunk)[1]);
            } catch (e) {
                type = null;
                count = null;
            }

            try {
                version = parseInt(/"version.?:\s?(\d*)/g.exec(chunk)[1]);
            } catch (e) {
                version = null;
            }

            resolve({
                count: count,
                type: type,
                version: version,
            });
        });
    });
}
