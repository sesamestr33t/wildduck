'use strict';
/* global db, log */
// MongoDB Migration Script: Add parsed search fields (search.from, search.to, search.cc, search.subject, search.fromName) to messages
const config = require('@zone-eu/wild-config');

const ENABLED = process.env.NODE_ENV === 'test' ? false : !!config?.migrations?.database?.addSearchFields?.enabled;
const BATCH_SIZE = 1000;

async function addSearchFields() {
    log('Starting migration: Adding search fields to messages collection');

    const collection = db.collection('messages');

    // Get current max id — only migrate documents that exist at start
    const maxIdDoc = await collection.find({}).sort({ _id: -1 }).limit(1).toArray();
    const maxIdAtStart = maxIdDoc.length > 0 ? maxIdDoc[0]._id : null;

    if (!maxIdAtStart) {
        log('No documents found. Migration skipped.');
        return;
    }

    const totalToMigrate = await collection.countDocuments({
        search: { $exists: false },
        _id: { $lte: maxIdAtStart }
    });

    if (totalToMigrate === 0) {
        log('All documents already have search field. Migration skipped.');
        return;
    }

    log(`Migrating ${totalToMigrate} documents (up to _id: ${maxIdAtStart}) in batches of ${BATCH_SIZE}...`);

    let processedCount = 0;
    let batchNumber = 0;
    let lastId = null;
    let running = true;

    while (running) {
        const query = {
            search: { $exists: false },
            _id: { $lte: maxIdAtStart }
        };

        // cursor based pagination
        if (lastId) {
            query._id.$gt = lastId;
        }

        // Find batch of documents without the search field
        const batch = await collection
            .find(query, {
                projection: { _id: true, envelope: true, subject: true }
            })
            .sort({ _id: 1 })
            .limit(BATCH_SIZE)
            .toArray();

        if (batch.length === 0) {
            running = false;
            break;
        }

        // Build bulk operations
        const bulkOps = [];
        for (const doc of batch) {
            const search = buildSearchFromEnvelope(doc.envelope, doc.subject);
            bulkOps.push({
                updateOne: {
                    filter: { _id: doc._id },
                    update: { $set: { search } }
                }
            });
        }

        if (bulkOps.length) {
            const result = await collection.bulkWrite(bulkOps, { ordered: false });
            processedCount += result.modifiedCount;
        }

        batchNumber++;

        // Update lastId for next iteration
        lastId = batch[batch.length - 1]._id;

        if (totalToMigrate < 50000 || batchNumber % 10 === 0) {
            log(`Progress: Batch ${batchNumber} - ${processedCount}/${totalToMigrate} (${((processedCount / totalToMigrate) * 100).toFixed(1)}%)`);
        }
    }

    log(`Migration complete! Updated ${processedCount} documents`);
}

/**
 * Build search fields from the envelope structure.
 * Envelope format (from create-envelope.js):
 *   [0] = date, [1] = subject,
 *   [2] = from → [[name, null, user, domain], ...],
 *   [3] = sender, [4] = reply-to,
 *   [5] = to → [[name, null, user, domain], ...],
 *   [6] = cc → [[name, null, user, domain], ...],
 *   [7] = bcc, [8] = in-reply-to, [9] = message-id
 */
function buildSearchFromEnvelope(envelope, subjectField) {
    const search = {};
    if (!envelope) {
        return search;
    }

    // Extract from addresses
    const fromEntries = envelope[2];
    if (Array.isArray(fromEntries) && fromEntries.length) {
        const addresses = [];
        const names = [];
        for (const entry of fromEntries) {
            if (Array.isArray(entry) && entry[2] && entry[3]) {
                const addr = (entry[2] + '@' + entry[3]).toLowerCase().trim();
                if (addr.includes('@')) {
                    addresses.push(addr);
                }
            }
            if (Array.isArray(entry) && entry[0]) {
                const name = entry[0].toString().toLowerCase().trim();
                if (name) {
                    names.push(name);
                }
            }
        }
        if (addresses.length) {
            search.from = addresses;
        }
        if (names.length) {
            search.fromName = names.join(' ');
        }
    }

    // Extract to addresses and names
    const toEntries = envelope[5];
    if (Array.isArray(toEntries) && toEntries.length) {
        const addresses = [];
        const names = [];
        for (const entry of toEntries) {
            if (Array.isArray(entry) && entry[2] && entry[3]) {
                const addr = (entry[2] + '@' + entry[3]).toLowerCase().trim();
                if (addr.includes('@')) {
                    addresses.push(addr);
                }
            }
            if (Array.isArray(entry) && entry[0]) {
                const name = entry[0].toString().toLowerCase().trim();
                if (name) {
                    names.push(name);
                }
            }
        }
        if (addresses.length) {
            search.to = addresses;
        }
        if (names.length) {
            search.toName = names.join(' ');
        }
    }

    // Extract cc addresses and names
    const ccEntries = envelope[6];
    if (Array.isArray(ccEntries) && ccEntries.length) {
        const addresses = [];
        const names = [];
        for (const entry of ccEntries) {
            if (Array.isArray(entry) && entry[2] && entry[3]) {
                const addr = (entry[2] + '@' + entry[3]).toLowerCase().trim();
                if (addr.includes('@')) {
                    addresses.push(addr);
                }
            }
            if (Array.isArray(entry) && entry[0]) {
                const name = entry[0].toString().toLowerCase().trim();
                if (name) {
                    names.push(name);
                }
            }
        }
        if (addresses.length) {
            search.cc = addresses;
        }
        if (names.length) {
            search.ccName = names.join(' ');
        }
    }

    // Extract subject — prefer the top-level subject field (already decoded), fall back to envelope
    const subject = subjectField || (envelope[1] || '').toString();
    if (subject) {
        search.subject = subject.toLowerCase().trim();
    }

    return search;
}

if (ENABLED) {
    return addSearchFields();
}
