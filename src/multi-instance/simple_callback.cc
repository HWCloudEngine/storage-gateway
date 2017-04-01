#include <iterator>
#include <iostream>
#include "simple_callback.h"

void print(const list<string>& s)
{
    list<string>::const_iterator i;
    for(i = s.begin(); i != s.end(); i++){
        cout << *i << " ";
    }
    cout << endl;
}

void SimpleCallback::join_group_callback(const list<string> &old_buckets, const list<string> &new_buckets)
{
    cout << "join_group_callback, old buckets:";
    print(old_buckets);
    cout << "join_group_callback, new buckets:";
    print(new_buckets);

}

void SimpleCallback::leave_group_callback(const list<string> &old_buckets, const list<string> &new_buckets)
{
    cout << "leave_group_back, old buckets:";
    print(old_buckets);
    cout << "leave_group_back, new buckets:";
    print(new_buckets);
}