
#ifndef CONJUNCTIVEQ_DB_H
#define CONJUNCTIVEQ_DB_H
#include "Query.h"
#include <map>
#include <vector>
#include "Table.h"
#include "hopscotch_map.h"
#include "hopscotch_set.h"
#include <set>
#include "CDG.h"



using namespace std;


class CDG;

class Operation
{
public:
    static vector<string> table_id_2_name;

    enum Type{
        null,
        insert,
        delete_
    };

    Operation(string table_name, Tuple tuple, Type type, int ts = 0);
    Operation() = default;

    Type GetType() const ;

    const Tuple& GetTuple() const;

    const string& GetTableName() const;

    Operation GetDeleteVersion();
private:
    Type type = null;
    Tuple tuple;
    int table_id;
public:
    int timestamp = 0;
};



class DB
{
    friend class CDG;
public:
    Query query;
    // set<vector<int>> answer_set;
    int answer_num = 0;
    unsigned answer_set_peak_size = 0;
    int peak_oper_idx = -1;
    vector<shared_ptr<Table>> qv_id_2_table_pt;
    map<string, vector<int>> table_name_2_qv_ids;
    map<string, shared_ptr<Table>> table_name_2_pt;

    static int num_qvs;
    vector<vector<int>> basl_order;



    CDG cdg;


    static int num_oper;
public:
    /**
     * @brief set query and offline compute what cdg to get (if not baseline)
     * @param q
     */
    void SetQuery(Query q);

    void AddTuple(Tuple tuple, const string& table_name);

    void HandleOper(const Operation& operation);

    void ExecQuery(const Operation& oper, int tuple_id);

    vector<int> GetMatchingOrder(int);


    int GetCandidateSet(set<int>& candidate_set, int parent_qv, int parent_tuple, int cur_qv);
    int FilterCandidateSet(set<int>&, int, int, int);
    vector<pair<int,int>> GetParent(int qv, const vector<int>& matching_order, vector<int> vector2);




    void DFSEnum(const vector<int> &order, int step, vector<int>& tuple_ids,bool is_insert);

    vector<int> OrderTransform(const vector<int>& tuple_ids, const vector<int>& order);


    pair<unsigned int, unsigned int> GetNumAnswersAndPeak();

    unsigned GetTableSize(int qv_id);


    vector<int> PartialOrderTransform(const vector<int> &tuple_ids, const vector<int> &matched_order);
};

#endif //CONJUNCTIVEQ_DB_H
