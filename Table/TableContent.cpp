
#include "TableContent.h"
#include <fstream>


Content TableContent::GetAttr(int tuple_id, const vector<int>& attributes) {
    return table_content[tuple_id].GetAttr(attributes);
}

int TableContent::AddTuple(const Tuple& tuple) {
    if(dead_ids.empty()) {
        table_content.emplace_back(tuple);
        return table_content.size() - 1;
    }
    else
    {
        int id = dead_ids.top();
        dead_ids.pop();
        table_content[id] = tuple;
        return id;
    }
}

int TableContent::GetTupleNum() {
    return table_content.size() - dead_ids.size();
}

/**
 *
 * @param tuple_id
 * @return success
 */
bool TableContent::DeleteTuple(int tuple_id) {
    dead_ids.push(tuple_id);
    return true;
}

void TableContent::Print(int tuple_id) {
    if(tuple_id >= table_content.size())cout<<"invalid id\n";
    else table_content[tuple_id].Print();
}

void TableContent::Print(ostream &output) {

    for(auto& tuple: table_content)
    {
        tuple.Print(output);  /// Print() will check emptyness.
    }
}

Content Tuple::GetAttr(const vector<int>& attributes)const {
    Content ans;
//    cout<<"start tuple::getattr\n";
//    cout<<attributes.size()<<' ';
//    cout<<tuple_content.size()<<endl;
    for(const int attribute: attributes)
    {
//        cout<<"attr: "<<attribute<<endl;
        ans += tuple_content[attribute];
    }
    return ans;
}

void Tuple::AddContent(const char *str) {
    tuple_content.emplace_back(str);
}

bool Tuple::IsEmpty()const {
    return tuple_content.empty();
}

void Tuple::Print(ostream &output)const  {
    if(IsEmpty())return;

    for(const auto& attr: tuple_content)
    {
        attr.Print(output);
    }
    output<<endl;
}
