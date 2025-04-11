
#ifndef CONJUNCTIVEQ_INDEX_H
#define CONJUNCTIVEQ_INDEX_H


#include <vector>
#include "util.h"
#include "hopscotch_map.h"
#include "hopscotch_set.h"
#include <unordered_set>

class Index
{
    vector<int> attributes;
    
    tsl::hopscotch_map<Content, set<int>*, ContentHashFunc> hash_table;
    set<int> empty_set;

public:
    ~Index()
    {
        for(auto& [key, value]: hash_table)
        {
            delete value;
        }
    }

    const set<int>& GetTupleSet(const Content& content);

    void Insert(const Content& content, int i);

    const vector<int>& GetAttributes();

    void SetAttributes(const vector<int> &attributes_);

    bool Erase(Content key, int tuple_id);

    int GetTupleSetSize(const Content& content);

    int GetKeyNum()
    {
        return hash_table.size();
    }
};


#endif //CONJUNCTIVEQ_INDEX_H
