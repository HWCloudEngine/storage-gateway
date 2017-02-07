#ifndef ISYNCBARRIER_H
#define ISYNCBARRIER_H
#include <string>
#include <map>
using namespace std;

class ISyncBarrier
{
public:
    ISyncBarrier() = default;
    virtual ~ISyncBarrier(){}

    virtual void add_sync(const string& actor, const string& action) =  0;
    virtual void del_sync(const string& actor) = 0;

    /*true: action still on; false: action already over*/
    virtual bool check_sync_on(const string& actor) = 0;

};

#endif
