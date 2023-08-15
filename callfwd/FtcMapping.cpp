#include "FtcMapping.h"
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
DEFINE_uint32(ftc_f14map_prefetch, 16, "Maximum number of keys to prefetch");

struct PhoneList {
  uint64_t phone : 34, next : 30;
};
static constexpr uint64_t MAXROWS = (1 << 30) - 1;
static_assert(sizeof(PhoneList) == 8, "");

class FtcMapping::Data : public folly::hazptr_obj_base<FtcMapping::Data> {
 public:
  void getFtcs(size_t N, const uint64_t *pn, FtcData *Ftc) const;
  std::unique_ptr<Cursor> visitRows() const;
  void build();
  ~Data() noexcept;

  // metadata
  folly::dynamic meta;
  // pn->dn mapping
  folly::F14ValueMap<uint64_t, FtcData> dict;
  // pn column joined with sorted Ftc column
  std::vector<PhoneList> pnColumn;
  // unique-sorted Ftc column joined with pn
  std::vector<PhoneList> FtcIndex;
};

FtcMapping::Data::~Data() noexcept {
  LOG_IF(INFO, pnColumn.size() > 0) << "Reclaiming memory";
}

class FtcMapping::Cursor {
 public:
  virtual ~Cursor() = default;

  bool hasRow() const noexcept { return size_ != 0; }
  uint64_t currentPN() const noexcept { return pn_[pos_]; }
  FtcData currentFtc() const noexcept { return Ftc_[pos_]; }
  void prefetch(const Data *data) noexcept;
  void advance(const Data *data) noexcept;
  virtual void refill() = 0;

 protected:
  std::array<uint64_t, 8> pn_;
  std::array<FtcData, 8> Ftc_;
  unsigned size_;
  unsigned pos_;
};

void FtcMapping::Data::getFtcs(size_t N, const uint64_t *pn, FtcData *Ftc) const {
  folly::small_vector<folly::F14HashToken, 1> token;
  token.resize(std::min<size_t>(N, FLAGS_ftc_f14map_prefetch));
  while (N > 0) {
    size_t M = std::min<size_t>(N, FLAGS_ftc_f14map_prefetch);
  
    // Compute hash and prefetch buckets into CPU cache
    for (size_t i = 0; i < M; ++i) {
      token[i] = dict.prehash(pn[i]);
    }

    // Fill output vector
    for (size_t i = 0; i < M; ++i) {
      const auto it = dict.find(token[i], pn[i]);
      if (it != dict.cend()) {
        FtcData data = it->second;
        Ftc[i].pn = data.pn;
        Ftc[i].first_ftc_on = data.first_ftc_on;
        Ftc[i].last_ftc_on = data.last_ftc_on;
        Ftc[i].ftc_count = data.ftc_count;
      }
      else
        Ftc[i].pn = 0;
    }
    
    pn += M;
    Ftc += M;
    N -= M;
  }
}

void FtcMapping::getFtcs(size_t N, const uint64_t *pn, FtcData *Ftc) const {
  data_->getFtcs(N, pn, Ftc);
}

FtcData FtcMapping::getFtc(uint64_t pn) const {
  FtcData Ftc;
  getFtcs(1, &pn, &Ftc);
  return Ftc;
}

FtcMapping::Builder::Builder()
  : data_(std::make_unique<Data>())
{}

FtcMapping::Builder::~Builder() noexcept = default;

void FtcMapping::Builder::sizeHint(size_t numRecords) {
  data_->pnColumn.reserve(numRecords);
  data_->FtcIndex.reserve(numRecords);
  data_->dict.reserve(numRecords);
}

void FtcMapping::Builder::setMetadata(const folly::dynamic &meta) {
  data_->meta = meta;
}

FtcMapping::Builder& FtcMapping::Builder::addRow(std::vector<std::string> rowbuf) {
  uint64_t pn;

  //LOG(INFO) << "Add new row " << rowbuf[0];

  std::string phoneNumberStr = rowbuf[1];
  pn = std::stoll(phoneNumberStr);
  
  //LOG(INFO) << "Ftc_key method dict is " << pn;  
  if (data_->dict.count(pn))
    throw std::runtime_error("FtcMapping::Builder: duplicate key");
  if (data_->pnColumn.size() >= MAXROWS)
    throw std::runtime_error("FtcMapping::Builder: too much rows");

  FtcData data;
  data.pn = pn;
  data.first_ftc_on = rowbuf[2];
  data.last_ftc_on = rowbuf[3];
  data.ftc_count = rowbuf[5];
  
  data_->dict.emplace(pn, data);
  data_->pnColumn.push_back(PhoneList{pn, MAXROWS});
  data_->FtcIndex.push_back(PhoneList{pn, MAXROWS});
  return *this;
}

std::string FtcMapping::Builder::deleteCharacter(const std::string& input, char character) {
    std::string result;
    for (char c : input) {
        if (c != character) {
            result += c;
        }
    }
    return result;
}
void FtcMapping::Builder::fromCSV(std::istream &in, size_t &line, size_t limit) {
  std::string linebuf;
  std::string number;

  //LOG(INFO) << "FtcMapping fromCSV : ";

  for (limit += line; line < limit; ++line) {
    if (in.peek() == EOF)
      break;
    std::getline(in, linebuf);

    //LOG(INFO) << "pass first test";
    std::vector<std::string> parts;
    std::stringstream ss(linebuf);
    std::string part;

    while (std::getline(ss, part, ',')) {
        parts.push_back(part);
    }

    if (!(linebuf[0] >= '0' && linebuf[0] <= '9'))
      continue;

    //LOG(INFO) << "part size is " << parts.size();
    if (parts.size() >= 5) {
      addRow(parts); 
    }
    else
      throw std::runtime_error("bad number of columns");
  }
}

void FtcMapping::Data::build() {
  size_t N = pnColumn.size();

  // Connect FtcIndex_ with pnColumn_ before shuffling
  for (size_t i = 0; i < N; ++i)
    FtcIndex[i].next = i;

  static auto cmp = [](const PhoneList &lhs, const PhoneList &rhs) {
    if (lhs.phone == rhs.phone)
      return lhs.next < rhs.next;
    return lhs.phone < rhs.phone;
  };

#if HAVE_STD_PARALLEL
  std::stable_sort(std::execution::par_unseq, FtcIndex.begin(), FtcIndex.end(), cmp);
#else
  std::stable_sort(FtcIndex.begin(), FtcIndex.end(), cmp);
#endif

  // Wire pnColumn_ list by target
  for (size_t i = 0; i + 1 < N; ++i)
    pnColumn[FtcIndex[i].next].next = FtcIndex[i+1].next;

  static auto equal = [](const PhoneList &lhs, const PhoneList &rhs) {
    return lhs.phone == rhs.phone;
  };
  auto last = std::unique(FtcIndex.begin(), FtcIndex.end(), equal);
  FtcIndex.erase(last, FtcIndex.end());
  FtcIndex.shrink_to_fit();
}

FtcMapping FtcMapping::Builder::build() {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();
  return FtcMapping(std::move(data));
}

void FtcMapping::Builder::commit(std::atomic<Data*> &global) {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();

  size_t pn_count = data->pnColumn.size();
  size_t Ftc_count = data->FtcIndex.size();
  if (Data *veteran = global.exchange(data.release()))
    veteran->retire();
  LOG(INFO) << "Database updated: PNs=" << pn_count << " Ftcs=" << Ftc_count;
}

FtcMapping::FtcMapping(std::unique_ptr<Data> data) {
  CHECK(FLAGS_ftc_f14map_prefetch > 0);
  holder_.reset(data.get());
  data->retire();
  data_ = data.release();
}

FtcMapping::FtcMapping(std::atomic<Data*> &global)
  : data_(holder_.get_protected(global))
{
  CHECK(FLAGS_ftc_f14map_prefetch > 0);
}

FtcMapping::FtcMapping(FtcMapping&& rhs) noexcept = default;
FtcMapping::~FtcMapping() noexcept = default;

void FtcMapping::printMetadata() {
  if (!data_)
    return;
  LOG(INFO) << "Current mapping info:";
  for (auto kv : data_->meta.items())
    LOG(INFO) << "  " << kv.first.asString()
                  << ": " << folly::toJson(kv.second);
}

size_t FtcMapping::size() const noexcept {
  return data_->pnColumn.size();
}

