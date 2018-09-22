// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef INCLUDE_POLAR_STRING_H_
#define INCLUDE_POLAR_STRING_H_

#include <string.h>
#include <string>


namespace polar_race {

class PolarString {
 public:
  PolarString() : data_(""), size_(0) { }

  PolarString(const char* d, size_t n) : data_(d), size_(n) { }

  PolarString(const std::string& s) : data_(s.data()), size_(s.size()) { }

  PolarString(const char* s) : data_(s), size_(strlen(s)) { }

  const char* data() const { return data_; }

  size_t size() const { return size_; }

  bool empty() const { return size_ == 0; }

  char operator[](size_t n) const {
    return data_[n];
  }

  void clear() { data_ = ""; size_ = 0; }


  std::string ToString() const;

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const PolarString& b) const;

  bool starts_with(const PolarString& x) const {
    return ((size_ >= x.size_) &&
            (memcmp(data_, x.data_, x.size_) == 0));
  }

  bool ends_with(const PolarString& x) const {
    return ((size_ >= x.size_) &&
            (memcmp(data_ + size_ - x.size_, x.data_, x.size_) == 0));
  }

 private:
  const char* data_;
  size_t size_;
  // Intentionally copyable
};

inline bool operator==(const PolarString& x, const PolarString& y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const PolarString& x, const PolarString& y) {
  return !(x == y);
}

inline std::string PolarString::ToString() const {
  std::string result;
  result.assign(data_, size_);
  return result;
}

inline int PolarString::compare(const PolarString& b) const {
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = memcmp(data_, b.data_, min_len);
  if (r == 0) {
    if (size_ < b.size_) r = -1;
    else if (size_ > b.size_) r = +1;
  }
  return r;
}


}  // namespace polar_race

#endif  // INCLUDE_POLAR_STRING_H_
