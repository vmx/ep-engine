/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <cstdlib>
#include <cctype>
#include <algorithm>

#include <ep_engine.h>
#include "leveldb-kvstore.hh"
#include "vbucket.hh"

static const size_t DEFAULT_VAL_SIZE(64 * 1024);

LevelDBKVStore::LevelDBKVStore(EventuallyPersistentEngine &theEngine)
    : KVStore(),
      stats(theEngine.getEpStats()),
      valBuffer(NULL),
      valSize(0),
      batch(NULL),
      engine(theEngine) {
    keyBuffer = static_cast<char*>(calloc(1, sizeof(uint16_t)
                                          + std::numeric_limits<uint8_t>::max()));
    adjustValBuffer(DEFAULT_VAL_SIZE);
    open();
}

LevelDBKVStore::LevelDBKVStore(const LevelDBKVStore &from) : KVStore(from),
                                                             stats(from.stats),
                                                             valBuffer(NULL),
                                                             valSize(0),
                                                             batch(NULL),
                                                             engine(from.engine) {
    open();
    keyBuffer = static_cast<char*>(calloc(1, sizeof(uint16_t)
                                          + std::numeric_limits<uint8_t>::max()));
    adjustValBuffer(from.valSize);
}

void LevelDBKVStore::adjustValBuffer(const size_t to) {
    // Save room for the flags, exp, etc...
    size_t needed((sizeof(uint32_t)*2) + to);

    if (valBuffer == NULL || valSize < needed) {
        void *buf = realloc(valBuffer, needed);
        if (buf) {
            valBuffer = static_cast<char*>(buf);
            valSize = needed;
        }
    }
}

vbucket_map_t LevelDBKVStore::listPersistedVbuckets() {
    // TODO:  Something useful.
    std::map<std::pair<uint16_t, uint16_t>, vbucket_state> rv;
    return rv;
}

void LevelDBKVStore::set(const Item &itm, uint16_t vbid,
                         Callback<mutation_result> &cb) {
    leveldb::Slice k(mkKeySlice(vbid, itm.getKey()));
    leveldb::Slice v(mkValSlice(itm.getFlags(), itm.getExptime(),
                                itm.getNBytes(), itm.getData()));
    batch->Put(k, v);
    std::pair<int, int64_t> p(1, itm.getId() <= 0 ? 1 : 0);
    cb.callback(p);
}

void LevelDBKVStore::get(const std::string &key, uint64_t,
                         uint16_t vb, uint16_t, Callback<GetValue> &cb) {
    leveldb::Slice k(mkKeySlice(vb, key));
    std::string value;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), k, &value);
    if (!s.ok()) {
        GetValue rv(NULL, ENGINE_KEY_ENOENT);
        cb.callback(rv);
    }

    uint32_t flags, exp;
    size_t sz;
    const char *p;
    leveldb::Slice sval(value);
    grokValSlice(sval, &flags, &exp, &sz, &p);

    GetValue rv(new Item(key,
                         flags,
                         exp,
                         p,
                         sz,
                         0, // CAS
                         -1, // rowid
                         vb
                         ),
                ENGINE_SUCCESS, -1, 0);
    cb.callback(rv);
}

void LevelDBKVStore::reset() {
    if (db) {
        // TODO:  Implement.
    }
}

void LevelDBKVStore::del(const Item &itm,
                         uint64_t, uint16_t,
                         Callback<int> &cb) {
    leveldb::Slice k(mkKeySlice(itm.getVBucketId(), itm.getKey()));
    batch->Delete(k);
    int rv(1);
    cb.callback(rv);
}

bool LevelDBKVStore::delVBucket(uint16_t, uint16_t,
                                std::pair<int64_t, int64_t>) {
    abort(); // Should not be used
    return true;
}

static bool matches_prefix(leveldb::Slice s, size_t len, const char *p) {
    return s.size() >= len && std::memcmp(p, s.data(), len) == 0;
}

bool LevelDBKVStore::delVBucket(uint16_t vb, uint16_t) {
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    const char *prefix(reinterpret_cast<const char*>(&vb));
    std::string start(prefix, sizeof(vb));
    begin();
    for (it->Seek(start);
         it->Valid() && matches_prefix(it->key(), sizeof(vb), prefix);
         it->Next()) {
        batch->Delete(it->key());
    }
    delete it;
    commit();
    return true;
}

bool LevelDBKVStore::snapshotVBuckets(const vbucket_map_t &) {
    // TODO:  Implement
    return true;
}

bool LevelDBKVStore::snapshotStats(const std::map<std::string, std::string> &) {
    // TODO:  Implement
    return true;
}

void LevelDBKVStore::destroyInvalidVBuckets(bool) {
    // TODO:  implement
}

void LevelDBKVStore::dump(shared_ptr<Callback<GetValue> > cb) {
    std::cerr << "Loading up stuff." << std::endl;
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        uint16_t vbid;
        std::string key;
        uint32_t flags, exp;
        size_t sz;
        const char *p;
        grokKeySlice(it->key(), &vbid, &key);
        grokValSlice(it->value(), &flags, &exp, &sz, &p);

        GetValue rv(new Item(key,
                             flags,
                             exp,
                             p,
                             sz,
                             0, // CAS
                             -1, // rowid
                             vbid
                             ),
                    ENGINE_SUCCESS, -1, 0);
        cb->callback(rv);
    }
    assert(it->status().ok());  // Check for any errors found during the scan
    delete it;
}

void LevelDBKVStore::dump(uint16_t, shared_ptr<Callback<GetValue> >) {
    abort();
}

StorageProperties LevelDBKVStore::getStorageProperties() {
    size_t concurrency(1);
    StorageProperties rv(concurrency, concurrency - 1, 1, true, true);
    return rv;
}

leveldb::Slice LevelDBKVStore::mkKeySlice(uint16_t vbid, const std::string &k) {
    std::memcpy(keyBuffer, &vbid, sizeof(vbid));
    std::memcpy(keyBuffer + sizeof(vbid), k.data(), k.size());
    return leveldb::Slice(keyBuffer, sizeof(vbid) + k.size());
}

void LevelDBKVStore::grokKeySlice(const leveldb::Slice &s, uint16_t *v, std::string *k) {
    assert(s.size() > sizeof(uint16_t));
    std::memcpy(v, s.data(), sizeof(uint16_t));
    k->assign(s.data() + sizeof(uint16_t), s.size() - sizeof(uint16_t));
}

leveldb::Slice LevelDBKVStore::mkValSlice(uint32_t flags, uint32_t exp,
                                          size_t n, const void *p) {
    adjustValBuffer(n);
    std::memcpy(valBuffer, &flags, sizeof(flags));
    std::memcpy(valBuffer + sizeof(flags), &exp, sizeof(exp));
    std::memcpy(valBuffer + sizeof(flags) + sizeof(exp), p, n);
    return leveldb::Slice(valBuffer, sizeof(flags) + sizeof(exp) + n);
}

void LevelDBKVStore::grokValSlice(const leveldb::Slice &s, uint32_t *f, uint32_t *e,
                                  size_t *sz, const char **p) {
    assert(s.size() >= 2 * sizeof(uint32_t));
    std::memcpy(f, s.data(), sizeof(*f));
    std::memcpy(e, s.data() + sizeof(*f), sizeof(*f));
    size_t data_size(s.size() - (sizeof(*f) + sizeof(*e)));
    std::memcpy(sz, &data_size, sizeof(*sz));
    *p = s.data() + sizeof(*f) + sizeof(*e);
}
