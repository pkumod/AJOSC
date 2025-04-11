
#include "Index.h"
#include "util.h"

void Index::Insert(const Content& content, int tuple_id) {
    if(!hash_table.count(content))
    {
        hash_table.insert({content, new set<int>});
    }
    hash_table[content]->insert(tuple_id);
}

const vector<int>& Index::GetAttributes() {
    return attributes;
}

const set<int>& Index::GetTupleSet(const Content& content) {
    if (hash_table.count(content))
        return *(hash_table[content]);
    else return empty_set;

}


int Index::GetTupleSetSize(const Content& content) {
    auto iter = hash_table.find(content);
    if(iter == hash_table.end()) return 0;
    return iter->second->size();
}

void Index::SetAttributes(const vector<int> &attributes_) {
    attributes = attributes_;
}

/**
 *
 * @param key
 * @param tuple_id
 * @return success
 */
bool Index::Erase(Content key, int tuple_id) {
    if(!hash_table.count(key))return false;
    hash_table[key]->erase(tuple_id);
    if(hash_table[key]->empty())
    {
        delete hash_table[key];
        hash_table.erase(key);
    }
    return true;
}
