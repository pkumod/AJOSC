

#include "util.h"
#include "hopscotch_map.h"
#include "hopscotch_set.h"
#include <cmath>
#include <sstream>
#include "DB.h"
#include "random"



/**
 * @refitem https://blog.csdn.net/u011866460/article/details/40923381
 *          https://blog.csdn.net/qq_61988957/article/details/128242073
 * @param dir
 */
void mkdirs(const char *dir)
{
    int i,len;
    char str[1000];
    strcpy(str,dir);
    len = strlen(str);
    for(i = 0; i<len; i++)
    {
        if( str[i] == '/')
        {
            str[i] = 0;
            if(access(str,0) != 0)
            {
                mkdir(str,0777);
            }

            str[i] ='/';
        }
    }
    if( len > 0 && access(str ,0) != 0)
        mkdir(str,0777);
    assert(access(dir ,0) == 0);  /// the directory is set up successfully
}
size_t ContentHashFunc::operator()(const Content& content)const {
    return hash<string>()(content.str);
}

bool Content::operator==(const Content& content) const {
    return str == content.str;
}

Content& Content::operator+=(const Content &content) {
    str += content.str;
    return *this;
}

Content::Content(const char* str_){
    str = str_;
    str += '\0';  ///< add separator to avoid attr1 + attr2 = attr1' + attr2', while attr1 != attr1'. e.g. 5 + 56 = 55 + 6.
                    ///< separator is \0 because other things might appear in csv.
}

void Content::Print(ostream& output)const  {
    output<<str.substr(0,str.length()-1)<<',';
}

extern double scale_fac;




pair<int,int> GenOrderedPair(int id1, int id2)
{
    assert(id1!= id2);
    if(id1 <id2)return {id1,id2};
    return {id2,id1};
}

bool is_double(const string& str)
{
    char* end = nullptr;
    double val = strtod(str.c_str(), &end);
    if(end != str.c_str() && *end == '\0' && val != HUGE_VAL)
    {
        return std::count(str.begin(), str.end(),'.');
    }
    return false;
//    double y;
//    try {
//        y = stod(str);
//    }
//    catch(const exception&){
//        return false;
//    }
//    return true;
}


std::string get_csv_column(stringstream & in)
{
    std::string col;
    unsigned quotes = 0;
    char prev = 0;
    bool finis = false;
    for (int ch; !finis && (ch = in.get()) != EOF; ) {
        switch(ch) {
            case '"':
                if(prev != '\\') ++quotes;
                break;
            case ',':
                if (quotes == 0 || (prev == '"' && (quotes & 1) == 0)) {
                    finis = true;
                }
                break;
            default:;
        }
        col += prev = ch;
    }
    if(col.back() == ',')
    {
//        cout<<col<<endl;
//        assert(0);
        col = col.substr(0,col.size()-1);
//        cout<<col<<endl;
//        assert(0);
    }
    return col;
}
vector<string> GetCSVColumns(stringstream& in)
{
    vector<string> ans;
    if (!in) {
        assert(0);
    }
    for (std::string col; in; ) {
        col = get_csv_column(in);
        ans.emplace_back(col);
    }
    if (!in && !in.eof()) {
        assert(0);
    }
    return ans;
}