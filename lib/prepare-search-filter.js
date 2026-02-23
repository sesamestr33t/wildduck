'use strict';

const ObjectId = require('mongodb').ObjectId;
const { escapeRegexStr } = require('./tools');

const uidRangeStringToQuery = uidRange => {
    if (!uidRange) {
        return;
    }

    let query;

    if (/^\d+$/.test(uidRange)) {
        query = Number(uidRange);
    } else if (/^\d+(,\d+)*$/.test(uidRange)) {
        query = {
            $in: uidRange
                .split(',')
                .map(uid => Number(uid))
                .sort((a, b) => a - b)
        };
    } else if (/^\d+:(\d+|\*)$/.test(uidRange)) {
        let parts = uidRange
            .split(':')
            .map(uid => Number(uid))
            .sort((a, b) => {
                if (a === '*') {
                    return 1;
                }
                if (b === '*') {
                    return -1;
                }
                return a - b;
            });
        if (parts[0] === parts[1]) {
            query = parts[0];
        } else {
            query = {
                $gte: parts[0]
            };
            if (!isNaN(parts[1])) {
                query.$lte = parts[1];
            }
        }
    }
    return query;
};

const prepareSearchFilter = async (db, user, payload) => {
    const mailbox = payload.mailbox ? new ObjectId(payload.mailbox) : false;
    const idQuery = uidRangeStringToQuery(payload.id);
    const thread = payload.thread ? new ObjectId(payload.thread) : false;

    const orTerms = payload.or || {};
    const orQuery = [];

    const query = payload.query;
    const datestart = payload.datestart || false;
    const dateend = payload.dateend || false;
    const filterFrom = payload.from;
    const filterTo = payload.to;
    const filterSubject = payload.subject;
    const filterAttachments = payload.attachments;
    const filterFlagged = payload.flagged;
    const filterUnseen = payload.unseen;
    const filterSeen = payload.seen;
    const filterSearchable = payload.searchable;
    const filterMinSize = payload.minSize;
    const filterMaxSize = payload.maxSize;

    let userData;
    try {
        userData = await db.users.collection('users').findOne(
            {
                _id: user
            },
            {
                projection: {
                    username: true,
                    address: true,
                    specialUse: true
                }
            }
        );
    } catch (err) {
        err.responseCode = 500;
        err.code = 'InternalDatabaseError';
        err.formattedMessage = 'Database Error';
        throw err;
    }

    if (!userData) {
        let err = new Error('This user does not exist');
        err.responseCode = 404;
        err.code = 'UserNotFound';
        err.formattedMessage = 'This user does not exist';
        throw err;
    }

    // NB! Scattered query, searches over all user mailboxes and all shards
    let filter = {
        user
    };

    if (query) {
        filter.searchable = true;
        filter.$text = { $search: query };
    } else if (orTerms.query) {
        filter.searchable = true;
        orQuery.push({ $text: { $search: orTerms.query } });
    }

    if (mailbox) {
        filter.mailbox = mailbox;
    } else if (filterSearchable) {
        // filter out Trash and Junk
        let nonJunkAndTrashMailboxesCount;

        try {
            nonJunkAndTrashMailboxesCount = await db.database.collection('mailboxes').countDocuments({ user, specialUse: { $nin: ['\\Junk', '\\Trash'] } });
        } catch {
            // ignore
        }

        // Through trial and error it has been found that if the user has no more
        // than 200 mailboxes then the $in query is well optimized by the mongoDB
        // engine and can efficiently use current indexes and as such the query
        // becomes fast and efficient. If the user has more than 200 mailboxes
        // then the $in query becomes too large and mongoDB cannot optimize it well
        // anymore and has to default to collection scans and inefficient index use
        // resulting in a query slower than $nin.
        if (nonJunkAndTrashMailboxesCount && nonJunkAndTrashMailboxesCount < 200) {
            // use $in query
            try {
                const mailboxes = await db.database
                    .collection('mailboxes')
                    .find({ user, specialUse: { $nin: ['\\Junk', '\\Trash'] } })
                    .project({
                        _id: true
                    })
                    .toArray();
                filter.mailbox = { $in: mailboxes.map(m => m._id) };
            } catch (err) {
                err.responseCode = 500;
                err.code = 'InternalDatabaseError';
                err.formattedMessage = 'Database Error';
                throw err;
            }
        } else {
            // default to $nin
            try {
                const mailboxes = await db.database
                    .collection('mailboxes')
                    .find({ user, specialUse: { $in: ['\\Junk', '\\Trash'] } })
                    .project({
                        _id: true
                    })
                    .toArray();
                filter.mailbox = { $nin: mailboxes.map(m => m._id) };
            } catch (err) {
                err.responseCode = 500;
                err.code = 'InternalDatabaseError';
                err.formattedMessage = 'Database Error';
                throw err;
            }
        }
    }

    if (filter.mailbox && idQuery) {
        filter.uid = idQuery;
    }

    if (thread) {
        filter.thread = thread;
    }

    if (filterFlagged) {
        // mailbox is not needed as there's a special index for flagged messages
        filter.flagged = true;
    }

    if (filterSeen) {
        filter.unseen = false;
        filter.searchable = true;
    }

    // Unseen takes precedence over seen
    if (filterUnseen) {
        filter.unseen = true;
        filter.searchable = true;
    }

    if (filterSearchable) {
        filter.searchable = true;
    }

    if (datestart) {
        if (!filter.idate) {
            filter.idate = {};
        }
        filter.idate.$gte = datestart;
    }

    if (dateend) {
        if (!filter.idate) {
            filter.idate = {};
        }
        filter.idate.$lte = dateend;
    }

    if (filterFrom) {
        let term = filterFrom.toLowerCase().trim();
        if (!filter.$and) {
            filter.$and = [];
        }
        if (term.includes('@')) {
            filter.$and.push({ 'search.from': term });
        } else {
            let regex = escapeRegexStr(term);
            filter.$and.push({ 'search.fromName': { $regex: regex } });
        }
    }

    if (orTerms.from) {
        let term = orTerms.from.toLowerCase().trim();
        if (term.includes('@')) {
            orQuery.push({ 'search.from': term });
        } else {
            let regex = escapeRegexStr(term);
            orQuery.push({ 'search.fromName': { $regex: regex } });
        }
    }

    if (filterTo) {
        let term = filterTo.toLowerCase().trim();
        if (!filter.$and) {
            filter.$and = [];
        }
        if (term.includes('@')) {
            filter.$and.push({ $or: [{ 'search.to': term }, { 'search.cc': term }] });
        } else {
            let regex = escapeRegexStr(term);
            filter.$and.push({ $or: [{ 'search.toName': { $regex: regex } }, { 'search.ccName': { $regex: regex } }] });
        }
    }

    if (orTerms.to) {
        let term = orTerms.to.toLowerCase().trim();
        if (term.includes('@')) {
            orQuery.push({ $or: [{ 'search.to': term }, { 'search.cc': term }] });
        } else {
            let regex = escapeRegexStr(term);
            orQuery.push({ $or: [{ 'search.toName': { $regex: regex } }, { 'search.ccName': { $regex: regex } }] });
        }
    }

    if (filterSubject) {
        let term = filterSubject.toLowerCase().trim();
        let words = term.split(/\s+/).filter(w => w);
        if (!filter.$and) {
            filter.$and = [];
        }
        // Match each word as a prefix against the subjectWords array index
        for (let word of words) {
            filter.$and.push({ 'search.subjectWords': { $regex: '^' + escapeRegexStr(word) } });
        }
    }

    if (orTerms.subject) {
        let term = orTerms.subject.toLowerCase().trim();
        let words = term.split(/\s+/).filter(w => w);
        if (words.length === 1) {
            orQuery.push({ 'search.subjectWords': { $regex: '^' + escapeRegexStr(words[0]) } });
        } else {
            orQuery.push({ $and: words.map(w => ({ 'search.subjectWords': { $regex: '^' + escapeRegexStr(w) } })) });
        }
    }

    if (filterAttachments) {
        filter.ha = true;
    }

    if (filterMinSize) {
        filter.size = filter.size || {};
        filter.size.$gte = filterMinSize;
    }

    if (filterMaxSize) {
        filter.size = filter.size || {};
        filter.size.$lte = filterMaxSize;
    }

    if (orQuery.length) {
        filter.$or = orQuery;
    }

    return { filter, query };
};

module.exports = { uidRangeStringToQuery, prepareSearchFilter };
