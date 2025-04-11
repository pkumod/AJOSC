

#include "DB.h"
#include <utility>
#include <vector>
#include "hopscotch_map.h"
#include "hopscotch_set.h"
#include <cassert>
#include <fstream>
#include <chrono>
#include <unordered_map>
#include "random"
#include "unordered_set"



extern unsigned sliding_window_time_range;
int DFStimes = 0;

chrono::duration<double> monitor_time_delay;
int monitor_time_delay_test_num = 0;

vector<string> Operation::table_id_2_name;
vector<double> decide_latencies;
#define reorder_batch_size 1000
extern int all_oper_num;
int DB::num_qvs = -1;
bool need_decide_order = true;
extern int start_period_length; 
extern unsigned sliding_window_size;
extern vector<pair<int, int>> deg_changed_much;
extern unordered_set<CDGEdge *> w_changed_much;

int DB::num_oper = 0;

extern std::ranlux24_base r;
extern unsigned epsilon_reciprocal;
unsigned long long  answer_set_size_sum = 0;
int prev_answer_set_size = 0;
string prev_table_name;
extern string biggest_table;

bool prev_is_add = false;
extern int oper_idx;
void DB::HandleOper(const Operation& oper)
{
    answer_set_size_sum += abs(prev_answer_set_size- answer_num);

    prev_is_add = (oper.GetType() == Operation::Type::insert);
    prev_answer_set_size = answer_num;
    
    

    if (num_oper == start_period_length || num_oper == 0)
    {
        cdg.DecideOrder();
        need_decide_order = false;
        w_changed_much.clear();
        deg_changed_much.clear();
        
    }

    else if (num_oper > start_period_length)
    {
        if (need_decide_order)
        {
            bool really_need_recompute = true;
            really_need_recompute = cdg.MonitorStat();

            if (really_need_recompute)
            {
                
                cdg.IncrDecideOrder();
            }
        }
    }



    prev_table_name = oper.GetTableName();

    if (!query.table_name_2_index_info.count(oper.GetTableName()))
    {
        num_oper ++;
        return;
    }
    if (oper.GetType() == Operation::Type::insert)
    {
        int tuple_id = table_name_2_pt[oper.GetTableName()]->AddTuple(oper.GetTuple());
        ExecQuery(oper, tuple_id);
    }
    else
    {
        assert(oper.GetType() == Operation::Type::delete_);
        int tuple_id = table_name_2_pt[oper.GetTableName()]->GetTupleId(oper.GetTuple());
        ExecQuery(oper, tuple_id);
        bool success = table_name_2_pt[oper.GetTableName()]->DeleteTuple(tuple_id);
        assert(success);
    }


    num_oper++;
}

void DB::ExecQuery(const Operation& oper, int tuple_id)
{
    
    string start_table = oper.GetTableName();
    // cout<<"exec query: "<<start_table<<endl;
    for (int start_qv : table_name_2_qv_ids[start_table])
    {
        vector<int> order = GetMatchingOrder(start_qv);


        if (r() % epsilon_reciprocal == 0)
        {
            order = query.GenRandomOrder(start_qv);
        }

        vector<int> tuple_ids;
        tuple_ids.emplace_back(tuple_id);

        if (num_oper > start_period_length)
        {
            DFSEnum(order, 1, tuple_ids, oper.GetType() == Operation::insert);
        }
        else
        {
            DFSEnum(order, 1, tuple_ids, oper.GetType() == Operation::insert);
        }
    }
}

chrono::duration<double> useless;
extern int oper_idx;
extern clock_t start_time;
/**
 *
 * @param order
 * @param step
 * @param tuple_ids corresponds to "const vector<int>& order" respectively
 * @param is_insert true: insert; false: delete
 */
void DB::DFSEnum(const vector<int> &order, int step, vector<int> &tuple_ids, bool is_insert)
{
    DFStimes++;
    if((DFStimes & 65536) == 0 && (clock() - start_time) / (double)CLOCKS_PER_SEC > 3600*4)  
    {
        cout<<"take more than 4h to process. killed.\n";
        exit(0);
    }
 

    if (step == DB::num_qvs)
    {
        
        if (is_insert)
        {
            answer_num++;
            if (answer_num > answer_set_peak_size)
            {
                answer_set_peak_size = answer_num;
                peak_oper_idx = oper_idx;
            }
        }
        else
            answer_num--;
        return;
    }

    int cur_qv = order[step];
    vector<pair<int, int>> parents_qv_tuple = GetParent(cur_qv, order, tuple_ids);

    bool first = true;
    set<int> candidate_set;

    int ctuple_sum = 0, candidate_tuple_num = 0;

    for (auto [parent_qv, parent_tuple] : parents_qv_tuple)
    {
        
        if (first)
            candidate_tuple_num = GetCandidateSet(candidate_set, parent_qv, parent_tuple, cur_qv);
        else
            candidate_tuple_num = FilterCandidateSet(candidate_set, parent_qv, parent_tuple, cur_qv);

        cdg.UpdateDeg(candidate_tuple_num, parent_qv, cur_qv);
        ctuple_sum += candidate_tuple_num;
        first = false;
    }


    if (ctuple_sum != 0)
    {
        double w = (double)candidate_set.size() / (double)ctuple_sum;
        cdg.UpdateW(w, order, cur_qv, step);
    }
    for (int tuple : candidate_set)
    {
        tuple_ids.emplace_back(tuple);
        DFSEnum(order, step + 1, tuple_ids, is_insert);
        tuple_ids.pop_back();
    }
    
}

int debug_num = 0;

vector<pair<int, int>> DB::GetParent(int qv, const vector<int> &matching_order, vector<int> tuple_ids)
{
    vector<pair<int, int>> ans;
    int idx = 0;
    for (int matched_qv : matching_order)
    {
        if (qv == matched_qv)
            break;

        if (query.edge_infos.count(GenOrderedPair(matched_qv, qv)))
        {
            ans.emplace_back(matched_qv, tuple_ids[idx]);
        }
        idx++;
    }
    return ans;
}

vector<int> DB::OrderTransform(const vector<int> &tuple_ids, const vector<int> &order)
{
    vector<int> idxes;
    idxes.resize(order.size());
    int idx = 0;
    for (int table : order)
    {
        idxes[table] = idx;
        idx++;
    }
    vector<int> ans;
    ans.reserve(order.size());
    for (int _idx : idxes)
    {
        ans.emplace_back(tuple_ids[_idx]);
    }
    return ans;
}
vector<int> DB::PartialOrderTransform(const vector<int> &tuple_ids, const vector<int> &matched_order)
{
    vector<int> idxes;
    idxes.resize(num_qvs, -1);
    int idx = 0;
    for (int qv : matched_order)
    {
        idxes[qv] = idx;
        idx++;
    }
    vector<int> ans;
    ans.reserve(num_qvs);
    for (int _idx : idxes)
    {
        if (_idx == -1)
            ans.emplace_back(-1);
        else
            ans.emplace_back(tuple_ids[_idx]);
    }
    return ans;
}


vector<int> DB::GetMatchingOrder(int start_qv)
{
    return cdg.GetMatchingOrder(start_qv);
}

void DB::AddTuple(Tuple tuple, const string &table_name)
{
    table_name_2_pt[table_name]->AddTuple(std::move(tuple));
}

void DB::SetQuery(Query q) 
{
    query = std::move(q);
    num_qvs = query.adj_list.size();

    /// init: tsl::hopscotch_map<int,Table*> qv_id_2_table_pt;
    tsl::hopscotch_map<string, shared_ptr<Table>> done_init_table_names;

    int qv_id = 0;
    for (auto &table_name : query.qv_id_2_table_name)
    {
        /// init table metadata: vector<int> Index::attributes;
        if (!done_init_table_names.count(table_name))
        {
            const auto &index_info = query.table_name_2_index_info[table_name];
            auto table_pt = make_shared<Table>();
            table_name_2_pt[table_name] = table_pt;
            table_pt->GenIndex(index_info);
            table_pt->SetPrimaryKeyName(query.table_name_2_pk_names[table_name]);
            done_init_table_names.insert({table_name, table_pt});
            table_name_2_qv_ids.insert({table_name, {}});
        }
        qv_id_2_table_pt.emplace_back(done_init_table_names[table_name]);
        table_name_2_qv_ids[table_name].emplace_back(qv_id);

        qv_id++;
    }

    cdg.Init(query, this);

}

int DB::GetCandidateSet(set<int> &candidate_set, int parent_qv, int parent_tuple, int cur_qv)
{
    int index_id = query.GetIndexId(parent_qv, cur_qv);
    auto index_pt = qv_id_2_table_pt[cur_qv]->GetIndex(index_id);

    const vector<int> &attributes = query.GetJoinAttr(parent_qv, cur_qv);

    auto key = qv_id_2_table_pt[parent_qv]->GetAttr(parent_tuple, attributes);

    candidate_set = index_pt->GetTupleSet(key);
    return candidate_set.size();
}

int DB::FilterCandidateSet(set<int> &candidate_set, int parent_qv, int parent_tuple, int cur_qv)
{

    int index_id = query.GetIndexId(parent_qv, cur_qv);
    auto index_pt = qv_id_2_table_pt[cur_qv]->GetIndex(index_id);

    auto attributes = query.GetJoinAttr(parent_qv, cur_qv);

    auto key = qv_id_2_table_pt[parent_qv]->GetAttr(parent_tuple, attributes);

    const auto& candidate_set2 = index_pt->GetTupleSet(key);

    /// join candidate_set and candidate_set2.
    set<int> candidate_set_final;
    for (int candidate : candidate_set)
    {
        if (candidate_set2.count(candidate))
            candidate_set_final.insert(candidate);
    }
    candidate_set_final.swap(candidate_set);

    return candidate_set2.size();
}

Operation::Operation(string table_name_, Tuple tuple_, Type type_, int ts) :  tuple(std::move(tuple_)), type(type_), timestamp(ts) {
    
    table_id = 0;
    for (auto& tab_name: table_id_2_name)
    {
        if(table_name_ == tab_name)
        {
            return;
        }
        table_id++;
    }    
    table_id = -1;
    
}

Operation Operation::GetDeleteVersion()
{
    return {GetTableName(), tuple, Operation::delete_, timestamp + (int)sliding_window_time_range};
}

Operation::Type Operation::GetType()const 
{
    return type;
}

const Tuple& Operation::GetTuple() const 
{
    return tuple;
}
string empty_string;
const string& Operation::GetTableName() const
{
    if(table_id == -1) return empty_string;
    return table_id_2_name[table_id];
}



unsigned DB::GetTableSize(int qv_id)
{
    return qv_id_2_table_pt.at(qv_id)->GetTupleNum();
}
