#include "LergMapping.h"
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
DEFINE_uint32(lerg_f14map_prefetch, 16, "Maximum number of keys to prefetch");

struct PhoneList {
  uint64_t phone : 34, next : 30;
};
static constexpr uint64_t MAXROWS = (1 << 30) - 1;
static_assert(sizeof(PhoneList) == 8, "");



class LergMapping::Data : public folly::hazptr_obj_base<LergMapping::Data> {
 public:
  void getLergs(size_t N, const uint64_t *pn, LergData *lerg) const;
  std::unique_ptr<Cursor> visitRows() const;
  void build();
  ~Data() noexcept;

  // metadata
  folly::dynamic meta;
  // pn->dn mapping
  folly::F14ValueMap<uint64_t, LergData> dic_npa_nxx_x;
  folly::F14ValueMap<uint64_t, LergData> dic_npa_nxx;
  // pn column joined with sorted lerg column
  std::vector<PhoneList> pnColumn;
  // unique-sorted lerg column joined with pn
  std::vector<PhoneList> lergIndex;
};

LergMapping::Data::~Data() noexcept {
  LOG_IF(INFO, pnColumn.size() > 0) << "Reclaiming memory";
}

class LergMapping::Cursor {
 public:
  virtual ~Cursor() = default;

  bool hasRow() const noexcept { return size_ != 0; }
  uint64_t currentPN() const noexcept { return pn_[pos_]; }
  LergData currentLerg() const noexcept { return lerg_[pos_]; }
  void prefetch(const Data *data) noexcept;
  void advance(const Data *data) noexcept;
  virtual void refill() = 0;

 protected:
  std::array<uint64_t, 8> pn_;
  std::array<LergData, 8> lerg_;
  unsigned size_;
  unsigned pos_;
};

void LergMapping::Data::getLergs(size_t N, const uint64_t *pn, LergData *lerg) const {
  folly::small_vector<folly::F14HashToken, 1> token_npa_nxx_x;
  token_npa_nxx_x.resize(std::min<size_t>(N, FLAGS_lerg_f14map_prefetch));

  folly::small_vector<folly::F14HashToken, 1> token_npa_nxx;
  token_npa_nxx.resize(std::min<size_t>(N, FLAGS_lerg_f14map_prefetch));

  while (N > 0) {
    size_t M = std::min<size_t>(N, FLAGS_lerg_f14map_prefetch);
    

    // Compute hash and prefetch buckets into CPU cache
    for (size_t i = 0; i < M; ++i) {
      uint64_t npa_nxx_x = pn[i] / 1000;
      token_npa_nxx_x[i] = dic_npa_nxx_x.prehash(npa_nxx_x);
      token_npa_nxx[i] = dic_npa_nxx.prehash(npa_nxx_x / 10);
    }

    // Fill output vector
    for (size_t i = 0; i < M; ++i) {
      uint64_t npa_nxx_x = pn[i] / 1000;
      const auto it = dic_npa_nxx_x.find(token_npa_nxx_x[i], npa_nxx_x);
      if (it != dic_npa_nxx_x.cend())
        lerg[i] = it->second;
      else
        lerg[i].lerg_key = 0;

      if (lerg[i].lerg_key == 0)
      {
        const auto it = dic_npa_nxx.find(token_npa_nxx[i], npa_nxx_x / 10);
        if (it != dic_npa_nxx.cend())
          lerg[i] = it->second;
        else
          lerg[i].lerg_key = 0;
      }
    }

    pn += M;
    lerg += M;
    N -= M;
  }
}

void LergMapping::getLergs(size_t N, const uint64_t *pn, LergData *lerg) const {
  data_->getLergs(N, pn, lerg);
}

LergData LergMapping::getLerg(uint64_t pn) const {
  LergData lerg;
  getLergs(1, &pn, &lerg);
  return lerg;
}

LergMapping::Builder::Builder()
  : data_(std::make_unique<Data>())
{}

LergMapping::Builder::~Builder() noexcept = default;

void LergMapping::Builder::sizeHint(size_t numRecords) {
  data_->pnColumn.reserve(numRecords);
  data_->lergIndex.reserve(numRecords);
  data_->dic_npa_nxx_x.reserve(numRecords);
  data_->dic_npa_nxx.reserve(numRecords);
}

void LergMapping::Builder::setMetadata(const folly::dynamic &meta) {
  data_->meta = meta;
}

LergMapping::Builder& LergMapping::Builder::addRow(std::vector<std::string> rowbuf) {
  uint64_t lerg_key;

  //LOG(INFO) << "Add new row ";

  if (rowbuf[2] == "")
  {
    lerg_key = std::stoll(rowbuf[0]) * 1000 + std::stoll(rowbuf[1]);

    //LOG(INFO) << "lerg_key method dic_npa_nxx is " << lerg_key;  
    if (data_->dic_npa_nxx.count(lerg_key))
      throw std::runtime_error("LergMapping::Builder: duplicate key");
    if (data_->pnColumn.size() >= MAXROWS)
      throw std::runtime_error("LergMapping::Builder: too much rows");

    LergData data;
    data.lerg_key = lerg_key;
    data.company = rowbuf[4];
    data.ocn = rowbuf[5];
    data.rate_center = rowbuf[6];
    data.ocn_type = rowbuf[7];
    data.lata = rowbuf[8];
    data.country = rowbuf[9];

    data_->dic_npa_nxx.emplace(lerg_key, data);
  }
  else
  {
    lerg_key = std::stoll(rowbuf[0]) * 10000 + std::stoll(rowbuf[1]) * 10 + std::stoll(rowbuf[2]);
  
    //LOG(INFO) << "lerg_key method dic_npa_nxx_x is " << lerg_key;  

    if (data_->dic_npa_nxx_x.count(lerg_key))
      throw std::runtime_error("LergMapping::Builder: duplicate key");
    if (data_->pnColumn.size() >= MAXROWS)
      throw std::runtime_error("LergMapping::Builder: too much rows");

    LergData data;
    data.lerg_key = lerg_key;
    data.company = rowbuf[4];
    data.ocn = rowbuf[5];
    data.rate_center = rowbuf[6];
    data.ocn_type = rowbuf[7];
    data.lata = rowbuf[8];
    data.country = rowbuf[9];

    data_->dic_npa_nxx_x.emplace(lerg_key, data);
  }

  data_->pnColumn.push_back(PhoneList{lerg_key, MAXROWS});
  data_->lergIndex.push_back(PhoneList{lerg_key, MAXROWS});
  return *this;
}

std::string LergMapping::Builder::deleteCharacter(const std::string& input, char character) {
    std::string result;
    for (char c : input) {
        if (c != character) {
            result += c;
        }
    }
    return result;
}
void LergMapping::Builder::fromCSV(std::istream &in, size_t &line, size_t limit) {
  std::string linebuf;
  std::string number;

  //LOG(INFO) << "Lergmapping fromCSV : ";

  for (limit += line; line < limit; ++line) {
    if (in.peek() == EOF)
      break;
    std::getline(in, linebuf);
    
    if (!(linebuf[0] >= '0' && linebuf[0] <= '9'))
      continue;

    //LOG(INFO) << "pass first test";
    std::vector<std::string> parts;
    std::stringstream ss(linebuf);
    std::string part;

    while (std::getline(ss, part, ',')) {
        parts.push_back(part);
    }

    //LOG(INFO) << "part size is " << parts.size();
    if (parts.size() == 10) {
      //number = deleteCharacter(parts[0], '-');
      //uint64_t pn = std::stoll(number);
      addRow(parts); 
    }
    else
      throw std::runtime_error("bad number of columns");
  }
}

void LergMapping::Data::build() {
  size_t N = pnColumn.size();

  // Connect lergIndex_ with pnColumn_ before shuffling
  for (size_t i = 0; i < N; ++i)
    lergIndex[i].next = i;

  static auto cmp = [](const PhoneList &lhs, const PhoneList &rhs) {
    if (lhs.phone == rhs.phone)
      return lhs.next < rhs.next;
    return lhs.phone < rhs.phone;
  };

#if HAVE_STD_PARALLEL
  std::stable_sort(std::execution::par_unseq, lergIndex.begin(), lergIndex.end(), cmp);
#else
  std::stable_sort(lergIndex.begin(), lergIndex.end(), cmp);
#endif

  // Wire pnColumn_ list by target
  for (size_t i = 0; i + 1 < N; ++i)
    pnColumn[lergIndex[i].next].next = lergIndex[i+1].next;

  static auto equal = [](const PhoneList &lhs, const PhoneList &rhs) {
    return lhs.phone == rhs.phone;
  };
  auto last = std::unique(lergIndex.begin(), lergIndex.end(), equal);
  lergIndex.erase(last, lergIndex.end());
  lergIndex.shrink_to_fit();
}

LergMapping LergMapping::Builder::build() {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();
  return LergMapping(std::move(data));
}

void LergMapping::Builder::commit(std::atomic<Data*> &global) {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();

  size_t pn_count = data->pnColumn.size();
  size_t lerg_count = data->lergIndex.size();
  if (Data *veteran = global.exchange(data.release()))
    veteran->retire();
  LOG(INFO) << "Database updated: PNs=" << pn_count << " lergs=" << lerg_count;
}

LergMapping::LergMapping(std::unique_ptr<Data> data) {
  CHECK(FLAGS_lerg_f14map_prefetch > 0);
  holder_.reset(data.get());
  data->retire();
  data_ = data.release();
}

LergMapping::LergMapping(std::atomic<Data*> &global)
  : data_(holder_.get_protected(global))
{
  CHECK(FLAGS_lerg_f14map_prefetch > 0);
}

LergMapping::LergMapping(LergMapping&& rhs) noexcept = default;
LergMapping::~LergMapping() noexcept = default;

void LergMapping::printMetadata() {
  if (!data_)
    return;
  LOG(INFO) << "Current mapping info:";
  for (auto kv : data_->meta.items())
    LOG(INFO) << "  " << kv.first.asString()
                  << ": " << folly::toJson(kv.second);
}

size_t LergMapping::size() const noexcept {
  return data_->pnColumn.size();
}

