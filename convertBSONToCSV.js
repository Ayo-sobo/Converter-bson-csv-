const fs = require('fs');
const { createReadStream } = require('fs');
const { BSON } = require('bson');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const BSONStream = require('bson-stream');
const _ = require('lodash');

// Function to flatten objects
function flattenObject(ob) {
    const toReturn = {};

    for (const i in ob) {
        if (!ob.hasOwnProperty(i)) continue;

        if ((typeof ob[i]) === 'object' && ob[i] !== null && !Array.isArray(ob[i])) {
            const flatObject = flattenObject(ob[i]);
            for (const x in flatObject) {
                if (!flatObject.hasOwnProperty(x)) continue;
                toReturn[i + '.' + x] = flatObject[x];
            }
        } else {
            toReturn[i] = ob[i];
        }
    }
    return toReturn;
}

async function collectHeaders(bsonFilePath) {
    const bsonStream = createReadStream(bsonFilePath).pipe(new BSONStream());
    const allKeys = new Set();

    for await (const doc of bsonStream) {
        const flatDoc = flattenObject(doc);
        Object.keys(flatDoc).forEach(key => allKeys.add(key));
    }

    return Array.from(allKeys).map(key => ({ id: key, title: key }));
}

async function convertBSONToCSV(bsonFilePath, csvFilePath) {
    try {
        const headers = await collectHeaders(bsonFilePath);
        const csvWriter = createCsvWriter({
            path: csvFilePath,
            header: headers,
            append: false,
        });

        const bsonStream = createReadStream(bsonFilePath).pipe(new BSONStream());
        for await (const doc of bsonStream) {
            const flatDoc = flattenObject(doc);
            const record = {};
            headers.forEach(header => {
                record[header.id] = flatDoc[header.id] !== undefined ? flatDoc[header.id] : '';
            });
            await csvWriter.writeRecords([record]);
        }

        console.log(`CSV file written successfully to ${csvFilePath}`);
    } catch (error) {
        console.error('Error processing BSON data:', error);
    }
}

// Paths to your BSON file and the output CSV file
const bsonFilePath = '../bbbb/bbkup/consultations.bson'; // Replace with your BSON file path
const csvFilePath = '../new/newconsult.csv'; // Replace with your desired CSV file path
convertBSONToCSV(bsonFilePath, csvFilePath);
