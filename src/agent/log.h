#ifndef LOG_H
#define LOG_H

static inline char* file_name(char* path)
{
    size_t start;
    size_t len = strlen(path);
    start = len;
    while(start > 0)
    {
        if((path[start] == '/') || (path[start] == '\\')){
            start++; 
            break;
        }
        --start;
    }
    return &path[start];
}

#define FILE_NAME (file_name(__FILE__))

#define LOG_INFO(fmt, ...)  do { \
    printk(KERN_INFO "[%s %s %d] " fmt, FILE_NAME, __func__, __LINE__, ##__VA_ARGS__); \
}while(0)

#define LOG_ERR(fmt, ...)  do { \
    printk(KERN_ERR "[%s %s %d] " fmt, FILE_NAME, __func__, __LINE__, ##__VA_ARGS__); \
}while(0)

#endif
