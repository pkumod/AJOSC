

#ifndef CONJUNCTIVEQ_TABLE_H
#define CONJUNCTIVEQ_TABLE_H
#include "Index.h"
#include "TableContent.h"
#include <map>
#include "util.h"
#include "Query.h"

using namespace std;



class Table
{
public:
    map<int,Index*> indexes;
    TableContent table_content;
    vector<string> primary_key_names;
    double s_ub = 0;  ///< selectivity upper bound
    int stream_rate = 0; ///< = tuple num in this table
public:
    ~Table()
    {
        for(auto& [id,index]:indexes)
        {
            delete index;
        }
    }

    int AddTuple(const Tuple& tuple);

    Index* GetIndex(int index_id);

    Content GetAttr(int tuple, const vector<int>& attributes);

    void GenIndex(const IndexInfo &index_info);

    bool DeleteTuple(int tuple_id);

    int GetTupleNum();

    vector<string> GetPrimaryKeyName();

    int GetTupleId(Content primary_key_value);

    void SetPrimaryKeyName(const vector<string> & pk_names);

    int GetTupleId(const Tuple& tuple);

    Content GetPrimaryKeyVal(int tuple_id);

    Content GetPrimaryKeyVal(const Tuple& tuple);

    void PrintTableContent(ostream &table_name);

    
};



#endif //CONJUNCTIVEQ_TABLE_H
