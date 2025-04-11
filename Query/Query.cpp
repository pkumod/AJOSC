
#include "Query.h"
#include <random>
#include "hopscotch_map.h"
#include "hopscotch_set.h"
#include <fstream>
#include <sstream>
#include <cassert>
#include <algorithm>
#include "DB.h"
#include "chrono"

chrono::duration<double> gen_rand_order_time;
extern string primary_key_fn;
int gen_rand_order_times = 0;
/**
 *
 * @param nbr_qv
 * @param qv
 * @return the index id in qv, rather than nbr_qv !!!
 */
int Query::GetIndexId(int nbr_qv, int qv) {
    assert(edge_infos.count(GenOrderedPair(nbr_qv,qv)));
    auto edge_info = edge_infos[GenOrderedPair(nbr_qv,qv)];
    if(qv < nbr_qv) return edge_info.index_id1;
    else return edge_info.index_id2;
}

/**
 *
 * @param qv
 * @param nbr_qv
 * @return attribute_ids in qv, rather than nbr_qv
 * @note the order of attribute ids: should be the same with the corresponding attributes in nbr_qv::index.
 */
const vector<int>& Query::GetJoinAttr(int qv, int nbr_qv) {

    assert(edge_infos.count(GenOrderedPair(nbr_qv,qv)));
    auto& edge_info = edge_infos[GenOrderedPair(nbr_qv,qv)];

    if(qv < nbr_qv) return edge_info.join_attributes1;
    else return edge_info.join_attributes2;
}

void Query::InitEdge(map<int,int>& i_attr_2_j_attr, map<int,int>& j_attr_2_i_attr, int qv_i, int qv_j)
{
    if(!i_attr_2_j_attr.empty())
    {
        /// init these fields:
        ///    vector<vector<int>> adj_list;
        ///    map<pair<int,int>, EdgeInfo> edge_infos;  /// first: pair[qv1,qv2].
        ///    vector<IndexInfo> indexes_of_tables;

        adj_list[qv_i].emplace_back(qv_j);
        adj_list[qv_j].emplace_back(qv_i);

        vector<int> i_attrs,j_attrs; /// in increasing order
        vector<int> i_attrs_j_order, j_attrs_i_order;  /// used to init edge_info.join_attributes1/2. the order of edge_info.join_attributes1: should be the same with the corresponding attributes in qv 2's index.

        for(auto&[i_attr, j_attr]:i_attr_2_j_attr)
        {
            i_attrs.emplace_back(i_attr);
            j_attrs_i_order.emplace_back(j_attr);
        }
        for(auto&[j_attr, i_attr]:j_attr_2_i_attr)
        {
            j_attrs.emplace_back(j_attr);
            i_attrs_j_order.emplace_back(i_attr);
        }
        int i_index_id = table_name_2_index_info[qv_id_2_table_name[qv_i]].AddIndex(i_attrs);
        int j_index_id = table_name_2_index_info[qv_id_2_table_name[qv_j]].AddIndex(j_attrs);

        EdgeInfo edge_info;
        edge_info.index_id1 = i_index_id;
        edge_info.index_id2 = j_index_id;
        edge_info.join_attributes1 = i_attrs_j_order;
        edge_info.join_attributes2 = j_attrs_i_order;
        edge_infos.insert({{qv_i, qv_j}, edge_info});
    }
}

/**
 * @note before calling this func, we need to init: table_name_2_id.
 * @param filename
 * @return
 */
map<pair<int,int>,map<int,int>> Query::ReadEdgeInfo(ifstream& input, const tsl::hopscotch_map<string,tsl::hopscotch_map<string, int>>& table_name_2_attr_name_2_attr_id)
{
    map<pair<int,int>,map<int,int>> qv_pair_2_i_attr_2_j_attr;
    int qv_i,qv_j;
    string attr_i,attr_j;
    while(input>>qv_i>>attr_i>>qv_j>>attr_j)
    {
        int a_i = table_name_2_attr_name_2_attr_id.at(qv_id_2_table_name[qv_i]).at(attr_i);
        int a_j = table_name_2_attr_name_2_attr_id.at(qv_id_2_table_name[qv_j]).at(attr_j);
        if(qv_i>qv_j)
        {
            swap(qv_i,qv_j);
            swap(a_i,a_j);
        }
        if(!qv_pair_2_i_attr_2_j_attr.count({qv_i, qv_j}))
        {
            qv_pair_2_i_attr_2_j_attr.insert({{qv_i, qv_j}, {}});
        }
        qv_pair_2_i_attr_2_j_attr.at({qv_i, qv_j}).insert({a_i, a_j});
    }
    return qv_pair_2_i_attr_2_j_attr;
}


extern string query_file;

void Query::Init(const map<string, AttributeType>& attribute_2_type, const map<string, vector<string>>& table_2_attributes)
{

  

    tsl::hopscotch_map<string,tsl::hopscotch_map<string,int>> table_name_2_attr_name_2_id;
    for(auto& p: table_2_attributes)
    {
        table_name_2_attr_names.insert(p);
    }



    for(const auto& [table_name, attributes]: table_2_attributes)
    {
        table_name_2_attr_name_2_id.insert({table_name,{}});

        int attr_id = 0;
        for(auto& attribute_name: attributes)
        {
            table_name_2_attr_name_2_id[table_name].insert({attribute_name,attr_id});
            attr_id ++;
        }
    }


    
    ifstream input;
    input.open(query_file);
    


    int table_num;string tmp;
    input>>table_num;
    AddPrimaryKeyIndex(table_name_2_attr_name_2_id);  /// this func must be called before AddIndex other ways. The primary key's index_id should be 0!

    for( int i = 0;i<table_num;i++)input>>tmp;
    int qv_num;
    input>>qv_num;

    for( int i = 0;i<qv_num;i++)
    {
        int qv_id;
        string table_name;
        input>>qv_id>>table_name;
        qv_id_2_table_name.emplace_back(table_name);
    }
    adj_list.resize(qv_num);


    auto qv_pair_2_i_attr_2_j_attr = ReadEdgeInfo(input,table_name_2_attr_name_2_id);
    for(auto& [qv_pair, attr_map]: qv_pair_2_i_attr_2_j_attr)
    {
        map<int,int> j_attr_2_i_attr;
        for(auto& [attr_i,attr_j]:attr_map)
        {
            j_attr_2_i_attr.insert({attr_j,attr_i});
        }
        InitEdge(attr_map,j_attr_2_i_attr,qv_pair.first,qv_pair.second);
    }

    return;
    

}





void Query::AddPrimaryKeyIndex(const tsl::hopscotch_map<string,tsl::hopscotch_map<string,int>>& attribute_name_2_id) {
    ifstream input;
    
    input.open(primary_key_fn);
    string table_name;
    int attr_num;
    while (input>>table_name>>attr_num)
    {
        
        if(!attribute_name_2_id.count(table_name))
        {
            for (size_t i = 0; i < attr_num; i++)
            {
                string attr;
                input>>attr;
            }
            continue;
        }
        auto& attr_name2id = attribute_name_2_id.at(table_name);
        vector<string> primary_key_names;
        vector<int> attr_ids;
        for (size_t i = 0; i < attr_num; i++)
        {
            string attr;
            input>>attr;
            primary_key_names.emplace_back(attr);
            attr_ids.emplace_back(attr_name2id.at(attr));
        }

        table_name_2_pk_names.insert({table_name, primary_key_names});
        
        
        if(!table_name_2_index_info.count(table_name))
        {
            table_name_2_index_info.insert({table_name,{}});
        }
        table_name_2_index_info[table_name].AddIndex(attr_ids);
        
    }
    
    return;
    
    
    

}

///**
// *
// * @return vector[idx]: idx: start_table.
// */
vector<int> Query::GenRandomOrder(int start_qv)
{

    int qv_num = qv_id_2_table_name.size();
    vector<int> order;
    order.resize(qv_num);
    order[0] = start_qv;

    set<int> candidate_next_hop;
    set<int> visited_tables;

    int next_hop = start_qv;
    visited_tables.insert(start_qv);
//    cout<<"start table: "<<start_qv<<endl;
    for(int i = 1;i<qv_num;i++)
    {
//        cout<<"i: "<<i<<endl;
        for(int nbr: adj_list[next_hop])
        {
//            cout<<"nbr: "<<nbr<<endl;
            if(!visited_tables.count(nbr)) candidate_next_hop.insert(nbr);  /// if the table is added to candidates before: it will not be added again.
        }
        int num_candidates = candidate_next_hop.size();
        int candidate_idx = rand()%num_candidates;
        auto it = candidate_next_hop.begin();
        std::advance(it,candidate_idx);
        next_hop = *it;
        order[i]= next_hop;
        visited_tables.insert(next_hop);
        candidate_next_hop.erase(*it);
    }
    assert(candidate_next_hop.empty());

    return order;



}

/**
 * @note if the index has been added before, return the original index_id.
 * @param attr_ids  in increasing order
 * @return index_id.
 */
int IndexInfo::AddIndex(const vector<int>& attr_ids) {
     if(attributes_2_index_id.count(attr_ids))
     {
         return attributes_2_index_id[attr_ids];
     }
     else
     {
         int index_id = attributes_2_index_id.size();
         attributes_2_index_id.insert({attr_ids,index_id});
         return index_id;
     }
}
