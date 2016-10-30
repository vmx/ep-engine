/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef LEVELDB_KVSTORE_H
#define LEVELDB_KVSTORE_H 1

#include <map>
#include <vector>

#include <kvstore.h>

#include <leveldb/db.h>
#include <leveldb/slice.h>
#include <leveldb/write_batch.h>


/**
 * A persistence store based on leveldb.
 */
class LevelDBKVStore : public KVStore {
public:
    /**
     * Constructor
     *
     * @param config    Configuration information
     * @param read_only flag indicating if this kvstore instance is for read-only operations
     */
    LevelDBKVStore(KVStoreConfig &config);

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
    void reset(uint16_t vbucketId);

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
    void set(const Item &item, Callback<mutation_result> &cb);

    /**
     * Overrides get().
     */
    void get(const std::string &key, uint16_t vb, Callback<GetValue> &cb,
             bool fetchDelete = false);

    void getWithHeader(void* handle, const std::string& key,
                       uint16_t vb, Callback<GetValue>& cb,
                       bool fetchDelete = false);


    /**
     * Overrides del().
     */
    void del(const Item &itm, Callback<int> &cb);

    bool delVBucket(uint16_t vbucket);

    bool delVBucket(uint16_t vbucket, uint16_t vb_version,
                    std::pair<int64_t, int64_t> row_range);

    std::vector<vbucket_state *> listPersistedVbuckets(void);

    /**
     * Take a snapshot of the stats in the main DB.
     */
    bool snapshotStats(const std::map<std::string, std::string> &m);
    /**
     * Take a snapshot of the vbucket states in the main DB.
     */
    bool snapshotVBucket(uint16_t vbucketId, vbucket_state &vbstate,
                         VBStatePersist options);

    void destroyInvalidVBuckets(bool);

    size_t getNumShards() {
        return 1;
    }

    //size_t getShardId(const QueuedItem &) {
    //    return 0;
    //}

    void optimizeWrites(std::vector<queued_item> &) {
    }

    uint16_t getNumVbsPerFile(void) override {
        // TODO vmx 2016-10-29: return the actual value
        return 1024;
    }

    bool compactDB(compaction_ctx *) {
        // Explicit compaction is not needed
        return true;
    }

    uint16_t getDBFileId(const protocol_binary_request_compact_db&) {
        // Not needed if there is no explicit compaction
        return 0;
    }

    vbucket_state * getVBucketState(uint16_t vbucketId) {
        // TODO vmx 2016-10-29: implement
        std::string failovers("[{\"id\":0, \"seq\":0}]");
        return new vbucket_state(vbucket_state_dead, 0, 0, 0, 0,
                                 0, 0, 0, failovers);
    }

    size_t getNumPersistedDeletes(uint16_t vbid) {
        // TODO vmx 2016-10-29: implement
        return 0;
    }

    DBFileInfo getDbFileInfo(uint16_t vbid) {
        // TODO vmx 2016-10-29: implement
        DBFileInfo vbinfo;
        return vbinfo;
    }

    DBFileInfo getAggrDbFileInfo() {
        // TODO vmx 2016-10-29: implement
        DBFileInfo vbinfo;
        return vbinfo;
    }

    size_t getItemCount(uint16_t vbid) {
        // TODO vmx 2016-10-29: implement
        return 0;
    }

    RollbackResult rollback(uint16_t vbid, uint64_t rollbackSeqno,
                            std::shared_ptr<RollbackCB> cb) {
        // TODO vmx 2016-10-29: implement
        // NOTE vmx 2016-10-29: For LevelDB it will probably always be a
        // full rollback as it doesn't support Couchstore like rollback
        // semantics
        return RollbackResult(false, 0, 0, 0);
    }

    void pendingTasks() {
        // NOTE vmx 2016-10-29: Intentionally left empty;
    }

    ENGINE_ERROR_CODE getAllKeys(uint16_t vbid, std::string &start_key,
                                 uint32_t count,
                                 std::shared_ptr<Callback<uint16_t&, char*&> > cb) {
        // TODO vmx 2016-10-29: implement
        return ENGINE_SUCCESS;
    }

    ScanContext* initScanContext(std::shared_ptr<Callback<GetValue> > cb,
                                 std::shared_ptr<Callback<CacheLookup> > cl,
                                 uint16_t vbid, uint64_t startSeqno,
                                 DocumentFilter options,
                                 ValueFilter valOptions);

    scan_error_t scan(ScanContext* sctx) {
        // TODO vmx 2016-10-29: implement
        return scan_success;
    }

    void destroyScanContext(ScanContext* ctx) {
        // TODO vmx 2016-10-29: implement
        delete ctx;
    }

private:

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

    // Disallow assignment.
    void operator=(LevelDBKVStore &from);

    std::atomic<size_t> scanCounter; //atomic counter for generating scan id
};

#endif /* LEVELDB_KVSTORE_H */
