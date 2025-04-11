

#ifndef CONJUNCTIVEQ_UTIL_H
#define CONJUNCTIVEQ_UTIL_H

#include <cstdlib>
#include <string>
#include <iostream>
#include <set>
#include <vector>
#include<cstdio>
#include <sys/stat.h>
#include <sys/types.h>
#include<unistd.h>
#include <cassert>
#include <cstring>
#include <utility>
#include <vector>
#include "hopscotch_map.h"
#include "hopscotch_set.h"
#include <cassert>
#include <fstream>
#include <queue>
#include <algorithm>
#include <iostream>
#include <map>

using namespace std;


#define inf 2000000000
#define delta 0.05  
#define theta 0.04  


void mkdirs(const char *dir);

struct IndexInfo
{
    /// the first added index: the primary key.
    map<vector<int>,int> attributes_2_index_id; /// the attribute ids are in increasing order.
    int AddIndex(const vector<int>& attr_ids);
};

class Content{
    /// string/char*. tuple's content.
protected:
    string str;
    friend class ContentHashFunc;
public:
    bool operator==(const Content& content) const;
    Content& operator+=(const Content& content);
    explicit Content(const char* str_);
    Content() = default;

    void Print(ostream& output = cout)const ;
};

class ContentHashFunc{
public:
    size_t operator()(const Content& content) const;
};

enum AttributeType
{
    int_,double_,char_,date_,string_,
};



pair<int,int> GenOrderedPair(int id1, int id2);


bool is_double(const string& str);

vector<string> GetCSVColumns(stringstream& in);

#endif //CONJUNCTIVEQ_UTIL_H
