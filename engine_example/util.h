// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_SIMPLE_UTIL_H_
#define ENGINE_SIMPLE_UTIL_H_
#include <stdint.h>
#include <pthread.h>
#include <string>
#include <vector>

namespace polar_race {

// Hash
uint32_t StrHash(const char* s, int size);

// Env
int GetDirFiles(const std::string& dir, std::vector<std::string>* result);
int GetFileLength(const std::string& file);
int FileAppend(int fd, const std::string& value);
bool FileExists(const std::string& path);

// FileLock
class FileLock  {
 public:
    FileLock() {}
    virtual ~FileLock() {}

    int fd_;
    std::string name_;

 private:
    // No copying allowed
    FileLock(const FileLock&);
    void operator=(const FileLock&);
};

int LockFile(const std::string& f, FileLock** l);
int UnlockFile(FileLock* l);

}  // namespace polar_race

#endif  // ENGINE_SIMPLE_UTIL_H_
