#ifndef CALLFWD_LergMapping_H
#define CALLFWD_LergMapping_H

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

struct LergData {
  uint64_t lerg_key; // npa_nxx_x or npa_nxx
  std::string state;
  std::string company;
  std::string ocn;
  std::string rate_center;
  std::string ocn_type;
  std::string lata;
  std::string country;
};

class LergMapping {
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
    LergMapping build();

    /** Build indexes and commit data to global. */
    void commit(std::atomic<Data*> &global);

    /* delete specific character in the string*/
    std::string deleteCharacter(const std::string& input, char character);

  private:
    std::unique_ptr<Data> data_;
  };

  /** Construct taking ownership of Data. Used for tests. */
  LergMapping(std::unique_ptr<Data> data);
  /** Construct from globals and hold protected reference. */
  LergMapping(std::atomic<Data*> &global);
  /** Ensure move constructor exists */
  LergMapping(LergMapping&& rhs) noexcept;
  /** Get default LERG instance from global variable. */
  static LergMapping getLerg() noexcept;
  /** Check if DB fully loaded into memory. */
  static bool isAvailable() noexcept;
  ~LergMapping() noexcept;

  /** Get total number of records */
  size_t size() const noexcept;

  /** Log metadata to system journal */
  void printMetadata();

  /** Get a routing number from portability number.
    * If key wasn't found returns NONE. */
  LergData getLerg(uint64_t lerg) const;

  /** Get a routing number for a batch of keys.
    * Faster than calling getLerg() multiple times. */
  void getLergs(size_t N, const uint64_t *pn, LergData *lerg) const;

 private:
  folly::hazptr_holder<> holder_;
  const Data *data_;
  std::unique_ptr<Cursor> cursor_;
};

#endif // CALLFWD_LergMapping_H
