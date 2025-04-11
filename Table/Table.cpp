
#include "Table.h"
#include <cassert>



int  Table::AddTuple(const Tuple& tuple) {
    int tuple_id = table_content.AddTuple(tuple);
//    tuple.Print();
    for(auto [index_id, index_pt]: indexes)
    {
        auto attributes = index_pt->GetAttributes();
        auto key = GetAttr(tuple_id, attributes);
        index_pt->Insert(key, tuple_id);
    }
    return tuple_id;
}

Index* Table::GetIndex(int index_id) {
    if(!indexes.count(index_id)) return nullptr;
    return indexes[index_id];
}

Content Table::GetAttr(int tuple, const vector<int>& attributes) {
    return table_content.GetAttr(tuple, attributes);
}

void Table::GenIndex(const IndexInfo &index_info) {
    for(const auto& [attributes, index_id]: index_info.attributes_2_index_id)
    {
        indexes.insert({index_id, new Index});
        indexes[index_id]->SetAttributes(attributes);
    }
}

/**
 *
 * @param tuple
 * @return success or not
 */
bool Table::DeleteTuple(int tuple_id) {


    for(auto [index_id, index_pt]: indexes)
    {
        auto attributes = index_pt->GetAttributes();
        auto key = GetAttr(tuple_id, attributes);
        assert(index_pt->Erase(key, tuple_id));
    }
    assert(table_content.DeleteTuple(tuple_id));
    return true;
}

int Table::GetTupleNum() {
    return table_content.GetTupleNum();
}

vector<string> Table::GetPrimaryKeyName() {
    return primary_key_names;
}





int Table::GetTupleId(Content primary_key_value) {
    auto ans = indexes[0]->GetTupleSet(std::move(primary_key_value));
    assert(ans.size() <= 1);
    if(ans.empty())return -1;
    return *ans.begin();
}
int Table::GetTupleId(const Tuple& tuple)
{
    
    auto primary_key_content = GetPrimaryKeyVal(tuple);
    auto tuple_ids = indexes[0]->GetTupleSet(primary_key_content);

    if(tuple_ids.size() != 1)
    {
        for(auto id:tuple_ids)
        {
            cout<<"id: "<<id<<' ';
            table_content.Print(id);///todo
        }
        assert(0);
    }
    int tuple_id = *tuple_ids.begin();
    return tuple_id;
}

void Table::SetPrimaryKeyName(const vector<string> &pk_names) {
    primary_key_names = pk_names;
}

Content Table::GetPrimaryKeyVal(int tuple_id) {
    auto primary_key = indexes[0]->GetAttributes();
    return table_content.GetAttr(tuple_id, primary_key);

}
Content Table::GetPrimaryKeyVal(const Tuple& tuple) {
    auto primary_key = indexes[0]->GetAttributes();
    return tuple.GetAttr(primary_key);
}

void Table::PrintTableContent(ostream &output) {
    table_content.Print(output);
}
