#include "TollFreeMapping.h"
#include "PhoneMapping.h"

#include <algorithm>
#include <array>
#include <stdexcept>
#include <vector>
#if HAVE_STD_PARALLEL
#include <execution>
#endif
#include <glog/logging.h>
#include <folly/json.h>
#include <folly/dynamic.h>
#include <folly/Likely.h>
#include <folly/String.h>
#include <folly/Conv.h>
#include <folly/small_vector.h>
#include <folly/container/F14Map.h>
#include <folly/synchronization/Hazptr.h>
#include <folly/portability/GFlags.h>


// TODO: benchmark prefetch size
DEFINE_uint32(tollfree_f14map_prefetch, 16, "Maximum number of keys to prefetch");

struct PhoneList {
  uint64_t phone : 34, next : 30;
};
static constexpr uint64_t MAXROWS = (1 << 30) - 1;
static_assert(sizeof(PhoneList) == 8, "");

class TollFreeMapping::Data : public folly::hazptr_obj_base<TollFreeMapping::Data> {
 public:
  void getTollFrees(size_t N, const uint64_t *pn, uint64_t *tollfree) const;
  std::unique_ptr<Cursor> inverseTollFrees(uint64_t fromTollFree, uint64_t toTollFree) const;
  std::unique_ptr<Cursor> visitRows() const;
  void build();
  ~Data() noexcept;

  // metadata
  folly::dynamic meta;
  // pn->tollfree mapping
  folly::F14ValueMap<uint64_t, uint64_t> dict;
  // pn column joined with sorted tollfree column
  std::vector<PhoneList> pnColumn;
  // unique-sorted tollfree column joined with pn
  std::vector<PhoneList> tollfreeIndex;
};

TollFreeMapping::Data::~Data() noexcept {
  LOG_IF(INFO, pnColumn.size() > 0) << "Reclaiming memory";
}

class TollFreeMapping::Cursor {
 public:
  virtual ~Cursor() = default;

  bool hasRow() const noexcept { return size_ != 0; }
  uint64_t currentPN() const noexcept { return pn_[pos_]; }
  uint64_t currentTollFree() const noexcept { return tollfree_[pos_]; }
  void prefetch(const Data *data) noexcept;
  void advance(const Data *data) noexcept;
  virtual void refill() = 0;

 protected:
  std::array<uint64_t, 8> pn_;
  std::array<uint64_t, 8> tollfree_;
  unsigned size_;
  unsigned pos_;
};

void TollFreeMapping::Data::getTollFrees(size_t N, const uint64_t *pn, uint64_t *tollfree) const {
  folly::small_vector<folly::F14HashToken, 1> token;
  token.resize(std::min<size_t>(N, FLAGS_tollfree_f14map_prefetch));

  while (N > 0) {
    size_t M = std::min<size_t>(N, FLAGS_tollfree_f14map_prefetch);

    // Compute hash and prefetch buckets into CPU cache
    for (size_t i = 0; i < M; ++i)
      token[i] = dict.prehash(pn[i]);

    // Fill output vector
    for (size_t i = 0; i < M; ++i) {
      const auto it = dict.find(token[i], pn[i]);
      if (it != dict.cend())
        tollfree[i] = 1;
      else
        tollfree[i] = 0;
    }

    pn += M;
    tollfree += M;
    N -= M;
  }
}

void TollFreeMapping::getTollFrees(size_t N, const uint64_t *pn, uint64_t *tollfree) const {
  data_->getTollFrees(N, pn, tollfree);
}

uint64_t TollFreeMapping::getTollFree(uint64_t pn) const {
  uint64_t tollfree;
  getTollFrees(1, &pn, &tollfree);
  return tollfree;
}

TollFreeMapping::Builder::Builder()
  : data_(std::make_unique<Data>())
{}

TollFreeMapping::Builder::~Builder() noexcept = default;

void TollFreeMapping::Builder::sizeHint(size_t numRecords) {
  data_->pnColumn.reserve(numRecords);
  data_->tollfreeIndex.reserve(numRecords);
  data_->dict.reserve(numRecords);
}

void TollFreeMapping::Builder::setMetadata(const folly::dynamic &meta) {
  data_->meta = meta;
}

TollFreeMapping::Builder& TollFreeMapping::Builder::addRow(uint64_t pn, uint64_t tollfree) {
  if (data_->dict.count(pn))
    throw std::runtime_error("TollFreeMapping::Builder: duplicate key");
  if (data_->pnColumn.size() >= MAXROWS)
    throw std::runtime_error("TollFreeMapping::Builder: too much rows");

  data_->dict.emplace(pn, tollfree);
  data_->pnColumn.push_back(PhoneList{pn, MAXROWS});
  data_->tollfreeIndex.push_back(PhoneList{tollfree, MAXROWS});
  return *this;
}

std::string TollFreeMapping::Builder::deleteCharacter(const std::string& input, char character) {
    std::string result;
    for (char c : input) {
        if (c != character) {
            result += c;
        }
    }
    return result;
}
void TollFreeMapping::Builder::fromCSV(std::istream &in, size_t &line, size_t limit) {
  std::string linebuf;

  for (limit += line; line < limit; ++line) {
    if (in.peek() == EOF)
      break;
    std::getline(in, linebuf);
    
    if (!(linebuf[0] >= '0' && linebuf[0] <= '9'))
      continue;

    std::vector<std::string> parts;
    std::stringstream ss(linebuf);
    std::string part;

    while (std::getline(ss, part, ',')) {
        parts.push_back(part);
    }
    if (parts.size() == 3) {
      std::string number = parts[0];
      uint64_t pn = std::stoll(number);
      addRow(pn, 1); // Only need phone number
    }
    else
      throw std::runtime_error("bad number of columns");
  }
}

void TollFreeMapping::Data::build() {
  size_t N = pnColumn.size();

  // Connect tollfreeIndex_ with pnColumn_ before shuffling
  for (size_t i = 0; i < N; ++i)
    tollfreeIndex[i].next = i;

  static auto cmp = [](const PhoneList &lhs, const PhoneList &rhs) {
    if (lhs.phone == rhs.phone)
      return lhs.next < rhs.next;
    return lhs.phone < rhs.phone;
  };

#if HAVE_STD_PARALLEL
  std::stable_sort(std::execution::par_unseq, tollfreeIndex.begin(), tollfreeIndex.end(), cmp);
#else
  std::stable_sort(tollfreeIndex.begin(), tollfreeIndex.end(), cmp);
#endif

  // Wire pnColumn_ list by target
  for (size_t i = 0; i + 1 < N; ++i)
    pnColumn[tollfreeIndex[i].next].next = tollfreeIndex[i+1].next;

  static auto equal = [](const PhoneList &lhs, const PhoneList &rhs) {
    return lhs.phone == rhs.phone;
  };
  auto last = std::unique(tollfreeIndex.begin(), tollfreeIndex.end(), equal);
  tollfreeIndex.erase(last, tollfreeIndex.end());
  tollfreeIndex.shrink_to_fit();
}

TollFreeMapping TollFreeMapping::Builder::build() {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();
  return TollFreeMapping(std::move(data));
}

void TollFreeMapping::Builder::commit(std::atomic<Data*> &global) {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();

  size_t pn_count = data->pnColumn.size();
  size_t tollfree_count = data->tollfreeIndex.size();
  if (Data *veteran = global.exchange(data.release()))
    veteran->retire();
  LOG(INFO) << "Database updated: PNs=" << pn_count << " TollFrees=" << tollfree_count;
}

TollFreeMapping::TollFreeMapping(std::unique_ptr<Data> data) {
  CHECK(FLAGS_tollfree_f14map_prefetch > 0);
  holder_.reset(data.get());
  data->retire();
  data_ = data.release();
}

TollFreeMapping::TollFreeMapping(std::atomic<Data*> &global)
  : data_(holder_.get_protected(global))
{
  CHECK(FLAGS_tollfree_f14map_prefetch > 0);
}

TollFreeMapping::TollFreeMapping(TollFreeMapping&& rhs) noexcept = default;
TollFreeMapping::~TollFreeMapping() noexcept = default;

void TollFreeMapping::printMetadata() {
  if (!data_)
    return;
  LOG(INFO) << "Current mapping info:";
  for (auto kv : data_->meta.items())
    LOG(INFO) << "  " << kv.first.asString()
                  << ": " << folly::toJson(kv.second);
}

size_t TollFreeMapping::size() const noexcept {
  return data_->pnColumn.size();
}

