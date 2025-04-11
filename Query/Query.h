

#ifndef CONJUNCTIVEQ_QUERY_H
#define CONJUNCTIVEQ_QUERY_H
#include <vector>
#include <map>
#include <string>
#include "util.h"
#include "hopscotch_map.h"
#include "hopscotch_set.h"
#include "Table.h"
using namespace std;

struct EdgeInfo
{
    int index_id1,index_id2;
    vector<int> join_attributes1, join_attributes2;  /// the order of join attribute ids in qv 1: should be the same with the corresponding attributes in qv 2's index.
};


struct OrderSet
{

    vector<vector<int>> orders;
public:
    void AddOrder(const vector<int>& order)
    {
        orders.emplace_back(order);
    }
    unsigned GetSize(){return orders.size();}
};
class Query {
public:
    vector<vector<int>> adj_list;
    map<pair<int,int>, EdgeInfo> edge_infos;  /// first: pair[qv1,qv2].
public:
    vector<string> qv_id_2_table_name; /// qv id is int, started from 0.
    tsl::hopscotch_map<string,IndexInfo> table_name_2_index_info;
    tsl::hopscotch_map<string,vector<string>> table_name_2_pk_names;   ///< pk_names: primary_key_names
    tsl::hopscotch_map<string,vector<string>> table_name_2_attr_names;

    friend class DB;
    friend class CDG;
    friend class StaticDP;

public:
    void InitSharedVars();
    int GetIndexId(int qv1, int qv2);

    const vector<int>& GetJoinAttr(int qv, int nbr_qv);

    void Init(const map<std::string, AttributeType>& attribute_2_type, const std::map<std::string, vector<std::string>>& table_2_attributes);

    void AddPrimaryKeyIndex(const tsl::hopscotch_map<string,tsl::hopscotch_map<string,int>>& );

 
    void InitEdge(map<int, int> &i_attr_2_j_attr, map<int, int> &j_attr_2_i_attr, int qv_i, int qv_j);


    map<pair<int, int>, map<int, int>> ReadEdgeInfo(ifstream &filename, const tsl::hopscotch_map<string,tsl::hopscotch_map<string, int>> &vec_attributes);

    vector<int> GenRandomOrder(int start_qv);
};


#endif //CONJUNCTIVEQ_QUERY_H
