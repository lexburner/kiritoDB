// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_EXAMPLE_DOOR_PLATE_H_
#define ENGINE_EXAMPLE_DOOR_PLATE_H_
#include <string.h>
#include <stdint.h>
#include <map>
#include <string>
#include "include/engine.h"
#include "data_store.h"

namespace polar_race {

static const uint32_t kMaxKeyLen = 32;

struct Item {
  Item() : key_size(0), in_use(0) {
  }
  Location location;
  char key[kMaxKeyLen];
  uint32_t key_size;
  uint8_t in_use;
};

// Hash index for key
class DoorPlate  {
 public:
    explicit DoorPlate(const std::string& path);
    ~DoorPlate();

    RetCode Init();

    RetCode AddOrUpdate(const std::string& key, const Location& l);

    RetCode Find(const std::string& key, Location *location);

    RetCode GetRangeLocation(const std::string& lower, const std::string& upper,
        std::map<std::string, Location> *locations);

 private:
    std::string dir_;
    int fd_;
    Item *items_;

    int CalcIndex(const std::string& key);
};

}  // namespace polar_race

#endif  // ENGINE_EXAMPLE_DOOR_PLATE_H_
