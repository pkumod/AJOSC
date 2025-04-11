#include <iostream>
#include "DB.h"
#include "Query.h"
#include <fstream>
#include <sstream>
#include <cassert>
#include <queue>
#include <chrono>
#include "IO.h"
#include "argument_helper.h"
#include "random"
using namespace std;

unsigned sliding_window_size = 0;
unsigned sliding_window_factor = 5;
unsigned sliding_window_time_range = 0;

clock_t start_time;
double scale_fac = 1;

int oper_idx = -1;
int output_db_oper_idx = -2;  ///< -2: do not output. -1: output random partial db(because # peak answers = 0.)
int all_oper_num = -1;

extern vector<double> selectivity_recorder;
extern vector<double> decide_latencies;
void Test(Buffer& input_buf, Query& query)
{
    DB db{};
    db.SetQuery(query);


    start_time = clock();
    oper_idx = 0;
    vector<double> latencies;
    all_oper_num = input_buf.GetOperNum();
    while(true) { 
        const auto& oper = input_buf.GetNextOper();
        if (oper.GetType() == Operation::Type::null) break;

        db.HandleOper(oper);


        if(oper_idx%10000 == 0){
            if((clock() - start_time) / (double)CLOCKS_PER_SEC > 3600*4)  /// more than two hours
            {
                cout<<"take more than 4h to process. killed.\n";
                exit(0);
            }
        }

        oper_idx++;
        if(oper_idx == start_period_length) {
            start_time = clock();
            latencies.clear();
            decide_latencies.clear();
        }
    }

    auto end_time = clock();
    double tail_lat_100;
    double tail_lat_10000;
    double tail_lat_1000;
    double max_lat;


    




    cout<<"time(sec): "<<(end_time - start_time) / (double)CLOCKS_PER_SEC <<endl;


}

unsigned epsilon_reciprocal = 100000;

string dynamic_stream_fn;
string schema_file;
string query_file;
string static_stream_fn;
string primary_key_fn;
struct InputParam
{
    string schema_fn;
    string query_fn;
    string st_stream_fn;
    string dyn_stream_fn;
    string pri_key_fn;
    int output_database_oper_idx = -2;

    int sliding_window_fac = 5;
    int sliding_window_size_ = 0;  ///< only one of sliding_window_fac and sliding_window_size_ is valid.
    int sliding_window_time_range_ = 0;
    int epsilon_reciprocal_ = 100000;

    void SetDefaultValue() const {

        schema_file=schema_fn;
        query_file=query_fn;
        static_stream_fn=st_stream_fn;
        dynamic_stream_fn=dyn_stream_fn;
        primary_key_fn = pri_key_fn;
        
        sliding_window_factor = sliding_window_fac;
        sliding_window_size = sliding_window_size_;
        sliding_window_time_range = sliding_window_time_range_;

        output_db_oper_idx = output_database_oper_idx;
        epsilon_reciprocal = epsilon_reciprocal_;
    }
}input_params;

void ArgumentParser(int argc, char ** argv, InputParam& input_param)
{
    dsr::Argument_helper ah;
    ah.new_string("schema_fn","",input_param.schema_fn);
    ah.new_string("query_fn","",input_param.query_fn);
    ah.new_string("static_stream_fn","",input_param.st_stream_fn);
    ah.new_string("dynamic_stream_fn","",input_param.dyn_stream_fn);
    ah.new_string("primary_key_fn","",input_param.pri_key_fn);

    // ah.new_named_int('s', "sliding_window_fac", "sliding window fac", "sliding window fac", input_param.sliding_window_fac);
    // ah.new_named_int('w', "sliding_window_size", "sliding_window_size", "sliding_window_size", input_param.sliding_window_size_);
    // ah.new_named_int('t', "sliding_window_time_range", "sliding_window_time_range", "sliding_window_time_range", input_param.sliding_window_time_range_);
    ah.new_named_int('l', "start_period_length", "start_period_length", "start_period_length", start_period_length);

    // ah.new_named_int('o', "output_db_oper_idx", "output_db_oper_idx", "output_db_oper_idx", input_param.output_database_oper_idx);
    // ah.new_named_int('e', "epsilon_reciprocal", "epsilon_reciprocal", "epsilon_reciprocal", input_param.epsilon_reciprocal_);


    ah.process(argc, argv);
    
}



std::ranlux24_base r;

int main(int argc, char* argv[]) {

    std::random_device rd;
    unsigned int seed = rd();
    r = ranlux24_base(seed);



    ArgumentParser(argc,argv,input_params);
    input_params.SetDefaultValue();



    Buffer input_buf;
    Query query;


    PrepareQuery(query);
    PrepareData(input_buf, query);
    

    

    Test(input_buf,query);
    return 0;
}


