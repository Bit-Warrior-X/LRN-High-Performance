#ifndef CALLFWD_DnoMapping_H
#define CALLFWD_DnoMapping_H

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


class DnoMapping {
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
    Builder& addRow(uint64_t pn, std::string dnotype, uint64_t dno);

    /** Add many rows from CSV text stream. */
    void fromCSV(std::istream &in, std::string dnotype, size_t& line, size_t limit);

    /** Build indexes and release the data. */
    DnoMapping build();

    /** Build indexes and commit data to global. */
    void commit(std::atomic<Data*> &global);

    /* delete specific character in the string*/
    std::string deleteCharacter(const std::string& input, char character);

  private:
    std::unique_ptr<Data> data_;
  };

  /** Construct taking ownership of Data. Used for tests. */
  DnoMapping(std::unique_ptr<Data> data);
  /** Construct from globals and hold protected reference. */
  DnoMapping(std::atomic<Data*> &global);
  /** Ensure move constructor exists */
  DnoMapping(DnoMapping&& rhs) noexcept;
  /** Get default dno instance from global variable. */
  static DnoMapping getDNO() noexcept;
  /** Check if DB fully loaded into memory. */
  static bool isAvailable() noexcept;
  ~DnoMapping() noexcept;

  /** Log metadata to system journal */
  void printMetadata();

  /** Get a routing number from portability number.
    * If key wasn't found returns NONE. */
  uint64_t getDNO(uint64_t pn) const;

  /** Get a routing number for a batch of keys.
    * Faster than calling getDNO() multiple times. */
  void getDNOs(size_t N, const uint64_t *pn, uint64_t *dno) const;

 private:
  folly::hazptr_holder<> holder_;
  const Data *data_;
  std::unique_ptr<Cursor> cursor_;
};

#endif // CALLFWD_DnoMapping_H
