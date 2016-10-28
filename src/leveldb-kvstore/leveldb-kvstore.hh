/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef LEVELDB_KVSTORE_H
#define LEVELDB_KVSTORE_H 1

#include <map>
#include <vector>

#include <kvstore.hh>

#include <leveldb/db.h>
#include <leveldb/slice.h>
#include <leveldb/write_batch.h>

class EventuallyPersistentEngine;
class EPStats;

/**
 * A persistence store based on leveldb.
 */
class LevelDBKVStore : public KVStore {
public:

    /**
     * Construct an instance of sqlite with the given database name.
     */
    LevelDBKVStore(EventuallyPersistentEngine &theEngine);

    /**
     * Copying opens a new underlying DB.
     */
    LevelDBKVStore(const LevelDBKVStore &from);

    /**
     * Cleanup.
     */
    ~LevelDBKVStore() {
        close();
        free(keyBuffer);
        free(valBuffer);
    }

    /**
     * Reset database to a clean state.
     */
    void reset();

    /**
     * Begin a transaction (if not already in one).
     */
    bool begin() {
        if(!batch) {
            batch = new leveldb::WriteBatch;
        }
        return batch != NULL;
    }

    /**
     * Commit a transaction (unless not currently in one).
     *
     * Returns false if the commit fails.
     */
    bool commit() {
        if(batch) {
            leveldb::Status s = db->Write(leveldb::WriteOptions(), batch);
            if (s.ok()) {
                delete batch;
                batch = NULL;
            }
        }
        return batch == NULL;
    }

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback() {
        if (batch) {
            delete batch;
        }
    }

    /**
     * Query the properties of the underlying storage.
     */
    StorageProperties getStorageProperties();

    /**
     * Overrides set().
     */
    void set(const Item &item, uint16_t vb_version, Callback<mutation_result> &cb);

    /**
     * Overrides get().
     */
    void get(const std::string &key, uint64_t rowid,
             uint16_t vb, uint16_t vbver, Callback<GetValue> &cb);

    /**
     * Overrides del().
     */
    void del(const Item &itm,
             uint64_t rowid, uint16_t vbver,
             Callback<int> &cb);

    bool delVBucket(uint16_t vbucket, uint16_t vb_version);

    bool delVBucket(uint16_t vbucket, uint16_t vb_version,
                    std::pair<int64_t, int64_t> row_range);

    vbucket_map_t listPersistedVbuckets(void);

    /**
     * Take a snapshot of the stats in the main DB.
     */
    bool snapshotStats(const std::map<std::string, std::string> &m);
    /**
     * Take a snapshot of the vbucket states in the main DB.
     */
    bool snapshotVBuckets(const vbucket_map_t &m);

    /**
     * Overrides dump
     */
    void dump(shared_ptr<Callback<GetValue> > cb);

    void dump(uint16_t vb, shared_ptr<Callback<GetValue> > cb);

    void destroyInvalidVBuckets(bool);

    size_t getNumShards() {
        return 1;
    }

    size_t getShardId(const QueuedItem &) {
        return 0;
    }

    void optimizeWrites(std::vector<queued_item> &) {
    }

private:

    EPStats &stats;

    /**
     * Direct access to the DB.
     */
    leveldb::DB* db;
    char *keyBuffer;
    char *valBuffer;
    size_t valSize;

    void open() {
        leveldb::Options options;
        options.create_if_missing = true;
        leveldb::Status s = leveldb::DB::Open(options, "/tmp/testdb", &db);
        assert(s.ok());
    }

    void close() {
        delete db;
        db = NULL;
    }

    leveldb::Slice mkKeySlice(uint16_t, const std::string &);
    void grokKeySlice(const leveldb::Slice &, uint16_t *, std::string *);

    void adjustValBuffer(const size_t);

    leveldb::Slice mkValSlice(uint32_t, uint32_t, size_t s, const void *);
    void grokValSlice(const leveldb::Slice &, uint32_t *, uint32_t *,
                      size_t *, const char **);

    leveldb::WriteBatch *batch;
    EventuallyPersistentEngine &engine;

    // Disallow assignment.
    void operator=(LevelDBKVStore &from);
};

#endif /* LEVELDB_KVSTORE_H */
