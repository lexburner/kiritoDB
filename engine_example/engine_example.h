// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_EXAMPLE_ENGINE_EXAMPLE_H_
#define ENGINE_EXAMPLE_ENGINE_EXAMPLE_H_
#include <pthread.h>
#include <string>
#include "include/engine.h"
#include "util.h"
#include "door_plate.h"
#include "data_store.h"

namespace polar_race {

class EngineExample : public Engine  {
 public:
  static RetCode Open(const std::string& name, Engine** eptr);

  explicit EngineExample(const std::string& dir)
    : mu_(PTHREAD_MUTEX_INITIALIZER),
    db_lock_(NULL), plate_(dir), store_(dir) {
    }

  ~EngineExample();

  RetCode Write(const PolarString& key,
      const PolarString& value) override;

  RetCode Read(const PolarString& key,
      std::string* value) override;

  RetCode Range(const PolarString& lower,
      const PolarString& upper,
      Visitor &visitor) override;

 private:
  pthread_mutex_t mu_;
  FileLock* db_lock_;
  DoorPlate plate_;
  DataStore store_;
};

}  // namespace polar_race

#endif  // ENGINE_EXAMPLE_ENGINE_EXAMPLE_H_
