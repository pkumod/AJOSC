
#ifndef CONJUNCTIVEQ_TABLECONTENT_H
#define CONJUNCTIVEQ_TABLECONTENT_H


#include <vector>
#include "util.h"
#include <string>
#include <stack>

class Tuple
{
    vector<Content> tuple_content;
public:
    Content GetAttr(const vector<int>& attributes)const;

    void AddContent(const char *str);

    void Print(ostream& output = cout)const;

    bool IsEmpty() const;
};

class TableContent
{
    vector<Tuple> table_content;
    stack<int> dead_ids;
public:

    Content GetAttr(int tuple_id, const vector<int>& attributes);

    int AddTuple(const Tuple& tuple);
    int GetTupleNum();

    bool DeleteTuple(int tuple_id);

    void Print(int tuple_id);

    void Print(ostream& output);
};
#endif //CONJUNCTIVEQ_TABLECONTENT_H
