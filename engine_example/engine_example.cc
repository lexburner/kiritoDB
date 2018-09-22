// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <map>
#include "util.h"
#include "engine_example.h"

namespace polar_race {

static const char kLockFile[] = "LOCK";

RetCode Engine::Open(const std::string& name, Engine** eptr) {
  return EngineExample::Open(name, eptr);
}

Engine::~Engine() {
}

RetCode EngineExample::Open(const std::string& name, Engine** eptr) {
  *eptr = NULL;
  EngineExample *engine_example = new EngineExample(name);

  RetCode ret = engine_example->plate_.Init();
  if (ret != kSucc) {
    delete engine_example;
    return ret;
  }
  ret = engine_example->store_.Init();
  if (ret != kSucc) {
    delete engine_example;
    return ret;
  }

  if (0 != LockFile(name + "/" + kLockFile, &(engine_example->db_lock_))) {
    delete engine_example;
    return kIOError;
  }

  *eptr = engine_example;
  return kSucc;
}

EngineExample::~EngineExample() {
  if (db_lock_) {
    UnlockFile(db_lock_);
  }
}

RetCode EngineExample::Write(const PolarString& key, const PolarString& value) {
  pthread_mutex_lock(&mu_);
  Location location;
  RetCode ret = store_.Append(value.ToString(), &location);
  if (ret == kSucc) {
    ret = plate_.AddOrUpdate(key.ToString(), location);
  }
  pthread_mutex_unlock(&mu_);
  return ret;
}

RetCode EngineExample::Read(const PolarString& key, std::string* value) {
  pthread_mutex_lock(&mu_);
  Location location;
  RetCode ret = plate_.Find(key.ToString(), &location);
  if (ret == kSucc) {
    value->clear();
    ret = store_.Read(location, value);
  }
  pthread_mutex_unlock(&mu_);
  return ret;
}

RetCode EngineExample::Range(const PolarString& lower, const PolarString& upper,
    Visitor &visitor) {
  pthread_mutex_lock(&mu_);
  std::map<std::string, Location> locations;
  RetCode ret =  plate_.GetRangeLocation(lower.ToString(), upper.ToString(), &locations);
  if (ret != kSucc) {
    pthread_mutex_unlock(&mu_);
    return ret;
  }

  std::string value;
  for (auto& pair : locations) {
    ret = store_.Read(pair.second, &value);
    if (kSucc != ret) {
      break;
    }
    visitor.Visit(pair.first, value);
  }
  pthread_mutex_unlock(&mu_);
  return ret;
}

}  // namespace polar_race

