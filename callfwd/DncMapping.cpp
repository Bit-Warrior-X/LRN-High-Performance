#include "DncMapping.h"
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
DEFINE_uint32(dnc_f14map_prefetch, 16, "Maximum number of keys to prefetch");

struct PhoneList {
  uint64_t phone : 34, next : 30;
};
static constexpr uint64_t MAXROWS = (1 << 30) - 1;
static_assert(sizeof(PhoneList) == 8, "");

class DncMapping::Data : public folly::hazptr_obj_base<DncMapping::Data> {
 public:
  void getDNCs(size_t N, const uint64_t *pn, uint64_t *dn) const;
  std::unique_ptr<Cursor> inverseDNCs(uint64_t fromDNC, uint64_t toDNC) const;
  std::unique_ptr<Cursor> visitRows() const;
  void build();
  ~Data() noexcept;

  // metadata
  folly::dynamic meta;
  // pn->dn mapping
  folly::F14ValueMap<uint64_t, uint64_t> dict;
  // pn column joined with sorted dnc column
  std::vector<PhoneList> pnColumn;
  // unique-sorted dnc column joined with pn
  std::vector<PhoneList> dncIndex;
};

DncMapping::Data::~Data() noexcept {
  LOG_IF(INFO, pnColumn.size() > 0) << "Reclaiming memory";
}

class DncMapping::Cursor {
 public:
  virtual ~Cursor() = default;

  bool hasRow() const noexcept { return size_ != 0; }
  uint64_t currentPN() const noexcept { return pn_[pos_]; }
  uint64_t currentDNC() const noexcept { return dnc_[pos_]; }
  void prefetch(const Data *data) noexcept;
  void advance(const Data *data) noexcept;
  virtual void refill() = 0;

 protected:
  std::array<uint64_t, 8> pn_;
  std::array<uint64_t, 8> dnc_;
  unsigned size_;
  unsigned pos_;
};

void DncMapping::Data::getDNCs(size_t N, const uint64_t *pn, uint64_t *dnc) const {
  folly::small_vector<folly::F14HashToken, 1> token;
  token.resize(std::min<size_t>(N, FLAGS_dnc_f14map_prefetch));

  while (N > 0) {
    size_t M = std::min<size_t>(N, FLAGS_dnc_f14map_prefetch);

    // Compute hash and prefetch buckets into CPU cache
    for (size_t i = 0; i < M; ++i)
      token[i] = dict.prehash(pn[i]);

    // Fill output vector
    for (size_t i = 0; i < M; ++i) {
      const auto it = dict.find(token[i], pn[i]);
      if (it != dict.cend())
        dnc[i] = 1;
      else
        dnc[i] = 0;
    }

    pn += M;
    dnc += M;
    N -= M;
  }
}

void DncMapping::getDNCs(size_t N, const uint64_t *pn, uint64_t *dnc) const {
  data_->getDNCs(N, pn, dnc);
}

uint64_t DncMapping::getDNC(uint64_t pn) const {
  uint64_t dnc;
  getDNCs(1, &pn, &dnc);
  return dnc;
}

DncMapping::Builder::Builder()
  : data_(std::make_unique<Data>())
{}

DncMapping::Builder::~Builder() noexcept = default;

void DncMapping::Builder::sizeHint(size_t numRecords) {
  data_->pnColumn.reserve(numRecords);
  data_->dncIndex.reserve(numRecords);
  data_->dict.reserve(numRecords);
}

void DncMapping::Builder::setMetadata(const folly::dynamic &meta) {
  data_->meta = meta;
}

DncMapping::Builder& DncMapping::Builder::addRow(uint64_t pn, uint64_t dnc) {
  if (data_->dict.count(pn))
    throw std::runtime_error("DncMapping::Builder: duplicate key");
  if (data_->pnColumn.size() >= MAXROWS)
    throw std::runtime_error("DncMapping::Builder: too much rows");

  data_->dict.emplace(pn, dnc);
  data_->pnColumn.push_back(PhoneList{pn, MAXROWS});
  data_->dncIndex.push_back(PhoneList{dnc, MAXROWS});
  return *this;
}

void DncMapping::Builder::fromCSV(std::istream &in, size_t &line, size_t limit) {
  std::string linebuf;
  std::vector<uint64_t> rowbuf;

  for (limit += line; line < limit; ++line) {
    if (in.peek() == EOF)
      break;
    std::getline(in, linebuf);
    rowbuf.clear();
    folly::split(',', linebuf, rowbuf);
    if (rowbuf.size() == 1)
      addRow(rowbuf[0], 1);
    else
      throw std::runtime_error("bad number of columns");
  }
}

void DncMapping::Data::build() {
  size_t N = pnColumn.size();

  // Connect dncIndex_ with pnColumn_ before shuffling
  for (size_t i = 0; i < N; ++i)
    dncIndex[i].next = i;

  static auto cmp = [](const PhoneList &lhs, const PhoneList &rhs) {
    if (lhs.phone == rhs.phone)
      return lhs.next < rhs.next;
    return lhs.phone < rhs.phone;
  };

#if HAVE_STD_PARALLEL
  std::stable_sort(std::execution::par_unseq, dncIndex.begin(), dncIndex.end(), cmp);
#else
  std::stable_sort(dncIndex.begin(), dncIndex.end(), cmp);
#endif

  // Wire pnColumn_ list by target
  for (size_t i = 0; i + 1 < N; ++i)
    pnColumn[dncIndex[i].next].next = dncIndex[i+1].next;

  static auto equal = [](const PhoneList &lhs, const PhoneList &rhs) {
    return lhs.phone == rhs.phone;
  };
  auto last = std::unique(dncIndex.begin(), dncIndex.end(), equal);
  dncIndex.erase(last, dncIndex.end());
  dncIndex.shrink_to_fit();
}

DncMapping DncMapping::Builder::build() {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();
  return DncMapping(std::move(data));
}

void DncMapping::Builder::commit(std::atomic<Data*> &global) {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();

  size_t pn_count = data->pnColumn.size();
  size_t dnc_count = data->dncIndex.size();
  if (Data *veteran = global.exchange(data.release()))
    veteran->retire();
  LOG(INFO) << "Database updated: PNs=" << pn_count << " DNCs=" << dnc_count;
}

DncMapping::DncMapping(std::unique_ptr<Data> data) {
  CHECK(FLAGS_dnc_f14map_prefetch > 0);
  holder_.reset(data.get());
  data->retire();
  data_ = data.release();
}

DncMapping::DncMapping(std::atomic<Data*> &global)
  : data_(holder_.get_protected(global))
{
  CHECK(FLAGS_dnc_f14map_prefetch > 0);
}

DncMapping::DncMapping(DncMapping&& rhs) noexcept = default;
DncMapping::~DncMapping() noexcept = default;

void DncMapping::printMetadata() {
  if (!data_)
    return;
  LOG(INFO) << "Current mapping info:";
  for (auto kv : data_->meta.items())
    LOG(INFO) << "  " << kv.first.asString()
                  << ": " << folly::toJson(kv.second);
}

size_t DncMapping::size() const noexcept {
  return data_->pnColumn.size();
}

