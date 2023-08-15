#ifndef CALLFWD_F606Mapping_H
#define CALLFWD_F606Mapping_H

#include <cstdint>
#include <memory>
#include <limits>
#include <cstddef>
#include <atomic>
#include <istream>
#include <string>

#include <folly/Range.h>
#include <folly/synchronization/HazptrHolder.h>

namespace folly { struct dynamic; }

struct F606Data {
  uint64_t pn;
  std::string last_F606_on;
  std::string first_F606_on;
};

class F606Mapping {
 public:
  class Data; /* opaque */
  class Cursor; /* opaque */

  class Builder {
  public:
    Builder();
    ~Builder() noexcept;

    /** Attach arbitrary metadata. */
    void setMetadata(const folly::dynamic &meta);

    /** Preallocate memory for expected number of records. */
    void sizeHint(size_t numRecords);

    /** Add a new row into the scratch buffer.
      * Throws `runtime_error` if key already exists. */
    Builder& addRow(std::vector<std::string> rowbuf);

    /** Add many rows from CSV text stream. */
    void fromCSV(std::istream &in, size_t& line, size_t limit);

    /** Build indexes and release the data. */
    F606Mapping build();

    /** Build indexes and commit data to global. */
    void commit(std::atomic<Data*> &global);

    /* delete specific character in the string*/
    std::string deleteCharacter(const std::string& input, char character);

  private:
    std::unique_ptr<Data> data_;
  };

  /** Construct taking ownership of Data. Used for tests. */
  F606Mapping(std::unique_ptr<Data> data);
  /** Construct from globals and hold protected reference. */
  F606Mapping(std::atomic<Data*> &global);
  /** Ensure move constructor exists */
  F606Mapping(F606Mapping&& rhs) noexcept;
  /** Get default F606 instance from global variable. */
  static F606Mapping getF606() noexcept;
  /** Check if DB fully loaded into memory. */
  static bool isAvailable() noexcept;
  ~F606Mapping() noexcept;

  /** Get total number of records */
  size_t size() const noexcept;

  /** Log metadata to system journal */
  void printMetadata();

  /** Get a routing number from portability number.
    * If key wasn't found returns NONE. */
  F606Data getF606(uint64_t pn) const;

  /** Get a routing number for a batch of keys.
    * Faster than calling getF606() multiple times. */
  void getF606s(size_t N, const uint64_t *pn, F606Data *F606) const;

 private:
  folly::hazptr_holder<> holder_;
  const Data *data_;
  std::unique_ptr<Cursor> cursor_;
};

#endif // CALLFWD_F606Mapping_H
