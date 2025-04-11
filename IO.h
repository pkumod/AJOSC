
#ifndef CONJUNCTIVEQ_IO_H
#define CONJUNCTIVEQ_IO_H

#include <iomanip>
#include "DB.h"
#include <algorithm>
#include "random"

extern double scale_fac;
extern unsigned sliding_window_size;
extern unsigned sliding_window_factor;
extern unsigned sliding_window_time_range;


extern string dynamic_stream_fn;
extern string schema_file;
extern string query_file;
extern string static_stream_fn;

int start_period_length = 10000000;
string biggest_table;

class Buffer
{
public:
    map<string, vector<Tuple>> table_name_2_tuples;
    map<int, string> end_offset_2_table_name;
    vector<Operation> operations_without_delete;
    map<string, int> table_name_2_next_tuple_id;
    int idx = 0;

    vector<Operation> operations;
    Operation empty_oper{};

    const Operation &GetNextOper();
    int GetOperNum()
    {
        return operations.size();
    }

    unsigned ReadDimTableTuples(const set<string> &dimension_table_names)
    {
        int num_tuples = 0;
        for (const string &table_name : dimension_table_names)
        {
            for (auto &tup : table_name_2_tuples[table_name])
            {
                table_name_2_next_tuple_id[table_name]++;
                Operation oper(table_name, tup, Operation::insert);
                operations_without_delete.emplace_back(oper);
                num_tuples++;
            }
            assert(table_name_2_next_tuple_id[table_name] == table_name_2_tuples[table_name].size());
        }
        return num_tuples;
    }


};

const Operation &Buffer::GetNextOper()
{

    if (idx >= operations.size())
        return empty_oper;

    const auto &ans = operations[idx];
    idx++;
    return ans;
}

void PrepareQuery(Query &query)
{
    map<string, AttributeType> attribute_2_type;
    map<string, vector<string>> table_2_attributes;
    char buf[100000];
    stringstream ss;

    ifstream query_input;

    ifstream schema_input;
    schema_input.open(schema_file);

    while (schema_input.getline(buf, 99999))
    {
        ss.clear();
        ss << buf;
        int tmp1;
        string table_name, tmp;
        ss >> table_name >> tmp1;
        vector<string> attributes;
        while (ss >> tmp)
        {
            attributes.emplace_back(tmp);
        }
        table_2_attributes.insert({table_name, attributes});
    }

    query_input.open(query_file);
    string t_name;
    set<string> used_table_name;
    map<string, vector<string>> used_table_2_attributes;
    int table_num;
    query_input >> table_num;
    for (int i = 0; i < table_num; i++)
    {
        query_input >> t_name;
        used_table_name.insert(t_name);
        used_table_2_attributes.insert({t_name, table_2_attributes.at(t_name)});
    }

    query.Init(attribute_2_type, used_table_2_attributes);
    return;

}

pair<bool, Operation> ReadTupleFromStreamFile(ifstream &file, bool with_type)
{

    char cbuf[100000];
    if (!file.getline(cbuf, 99999))
        return {false, {}};
    // cout<<cbuf<<endl;
    stringstream ss;

    Tuple tuple;
    ss.clear();
    ss << cbuf;
    bool is_insert = true;
    if (with_type)
        ss >> is_insert;
    string table_name;
    ss >> table_name;
    ss.get();

    auto columns = GetCSVColumns(ss);
    for (auto &col : columns)
    {
        // cout<<col<<endl;
        tuple.AddContent(col.c_str());
    }
    // exit(0);

    return {true, {table_name, tuple, is_insert ? Operation::insert : Operation::delete_}};
}

/**
 * @note: diff with ReadTupleFromStreamFile: here, the file split attributes with '|', and do not use "" to wrap contents containing ','.
 */
pair<bool, Operation> ReadTPCTupleFromStreamFile(ifstream &file, bool with_type)
{

    char cbuf[100000];
    if (!file.getline(cbuf, 99999))
        return {false, {}};
    char attr_buf[100000];
    // cout<<cbuf<<endl;
    stringstream ss;

    Tuple tuple;
    ss.clear();
    ss << cbuf;
    bool is_insert = true;
    if (with_type)
        ss >> is_insert;
    string table_name;
    ss >> table_name;
    ss.get();

    while (ss.getline(attr_buf, 99999, '|'))
    {
        // cout<<attr_buf<<endl;
        tuple.AddContent(attr_buf);
    }

    return {true, {table_name, tuple, is_insert ? Operation::insert : Operation::delete_}};
}
void PrepareData(Buffer &input_buf, const Query &query)
{
    for (auto &[table_name, _] : query.table_name_2_index_info)
    {
        Operation::table_id_2_name.emplace_back(table_name);
    }
    
    ifstream static_opers, dynamic_opers;
    static_opers.open(static_stream_fn);

    Tuple tmp;
    int valid_static_tuple_num = 0;
    while (true)
    {
        auto [not_eof, oper] = ReadTupleFromStreamFile(static_opers, false);
        if (!not_eof)
        {
            break;
        }
        
        input_buf.operations.emplace_back(oper);
        
    }

    static_opers.close();

    dynamic_opers.open(dynamic_stream_fn);

    assert(dynamic_opers.is_open());
    while (true)
    {
        auto [not_eof, oper] = ReadTupleFromStreamFile(dynamic_opers, true);
        if (!not_eof)
            break;
        input_buf.operations.emplace_back(oper);
    }

    dynamic_opers.close();
    
    return;
    
}

pair<unsigned, unsigned> DB::GetNumAnswersAndPeak()
{
    return {answer_num, answer_set_peak_size};
}

#endif // CONJUNCTIVEQ_IO_H
