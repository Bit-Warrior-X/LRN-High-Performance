#ifndef CALLFWD_DncMapping_H
#define CALLFWD_DncMapping_H

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


class DncMapping {
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
    Builder& addRow(uint64_t pn, uint64_t dnc);

    /** Add many rows from CSV text stream. */
    void fromCSV(std::istream &in, size_t& line, size_t limit);

    /** Build indexes and release the data. */
    DncMapping build();

    /** Build indexes and commit data to global. */
    void commit(std::atomic<Data*> &global);

    /* delete specific character in the string*/
    std::string deleteCharacter(const std::string& input, char character);

  private:
    std::unique_ptr<Data> data_;
  };

  /** Construct taking ownership of Data. Used for tests. */
  DncMapping(std::unique_ptr<Data> data);
  /** Construct from globals and hold protected reference. */
  DncMapping(std::atomic<Data*> &global);
  /** Ensure move constructor exists */
  DncMapping(DncMapping&& rhs) noexcept;
  /** Get default DNC instance from global variable. */
  static DncMapping getDNC() noexcept;
  /** Check if DB fully loaded into memory. */
  static bool isAvailable() noexcept;
  ~DncMapping() noexcept;

  /** Get total number of records */
  size_t size() const noexcept;

  /** Log metadata to system journal */
  void printMetadata();

  /** Get a routing number from portability number.
    * If key wasn't found returns NONE. */
  uint64_t getDNC(uint64_t pn) const;

  /** Get a routing number for a batch of keys.
    * Faster than calling getDNC() multiple times. */
  void getDNCs(size_t N, const uint64_t *pn, uint64_t *dnc) const;

 private:
  folly::hazptr_holder<> holder_;
  const Data *data_;
  std::unique_ptr<Cursor> cursor_;
};

#endif // CALLFWD_DncMapping_H
