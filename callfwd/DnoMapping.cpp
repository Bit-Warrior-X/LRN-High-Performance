#include "DnoMapping.h"
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
DEFINE_uint32(dno_f14map_prefetch, 16, "Maximum number of keys to prefetch");

struct PhoneList {
  uint64_t phone : 34, next : 30;
};
static constexpr uint64_t MAXROWS = (1 << 30) - 1;
static_assert(sizeof(PhoneList) == 8, "");

class DnoMapping::Data : public folly::hazptr_obj_base<DnoMapping::Data> {
 public:
  void getDNOs(size_t N, const uint64_t *pn, uint64_t *dn) const;
  void build();
  ~Data() noexcept;

  // metadata
  folly::dynamic meta;
  // pn->dn mapping
  folly::F14ValueMap<uint64_t, uint64_t> dict;
  folly::F14ValueMap<uint64_t, uint64_t> dict_npa;
  folly::F14ValueMap<uint64_t, uint64_t> dict_npa_nxx;
  folly::F14ValueMap<uint64_t, uint64_t> dict_npa_nxx_x;
};

DnoMapping::Data::~Data() noexcept {
  LOG(INFO) << "Reclaiming memory";
}

class DnoMapping::Cursor {
 public:
  virtual ~Cursor() = default;

  bool hasRow() const noexcept { return size_ != 0; }
  uint64_t currentPN() const noexcept { return pn_[pos_]; }
  uint64_t currentDNO() const noexcept { return dno_[pos_]; }
  void prefetch(const Data *data) noexcept;
  void advance(const Data *data) noexcept;
  virtual void refill() = 0;

 protected:
  std::array<uint64_t, 8> pn_;
  std::array<uint64_t, 8> dno_;
  unsigned size_;
  unsigned pos_;
};

void DnoMapping::Data::getDNOs(size_t N, const uint64_t *pn, uint64_t *dno) const {
  folly::small_vector<folly::F14HashToken, 1> token;
  token.resize(std::min<size_t>(N, FLAGS_dno_f14map_prefetch));
  
  folly::small_vector<folly::F14HashToken, 1> token_npa;
  token_npa.resize(std::min<size_t>(N, FLAGS_dno_f14map_prefetch));
  
  folly::small_vector<folly::F14HashToken, 1> token_npa_nxx;
  token_npa_nxx.resize(std::min<size_t>(N, FLAGS_dno_f14map_prefetch));
  
  folly::small_vector<folly::F14HashToken, 1> token_npa_nxx_x;
  token_npa_nxx_x.resize(std::min<size_t>(N, FLAGS_dno_f14map_prefetch));


  while (N > 0) {
    size_t M = std::min<size_t>(N, FLAGS_dno_f14map_prefetch);

    // Compute hash and prefetch buckets into CPU cache
    for (size_t i = 0; i < M; ++i) {
      token[i] = dict.prehash(pn[i]);
      token_npa[i] = dict_npa.prehash(pn[i] / 10000000);
      token_npa_nxx[i] = dict_npa_nxx.prehash(pn[i] / 10000);
      token_npa_nxx_x[i] = dict_npa_nxx_x.prehash(pn[i] / 1000);
    }

    // Fill output vector
    for (size_t i = 0; i < M; ++i) {
      if (dno[i] == 0) {
        const auto it = dict_npa.find(token_npa[i], pn[i] / 10000000);
        if (it != dict_npa.cend())
          dno[i] = 1;
        else
          dno[i] = 0;
      }
      if (dno[i] == 0) {
        const auto it = dict_npa_nxx.find(token_npa_nxx[i], pn[i] / 10000);
        if (it != dict.cend())
          dno[i] = 1;
        else
          dno[i] = 0;
      }
      if (dno[i] == 0) {
        const auto it = dict_npa_nxx_x.find(token_npa_nxx_x[i], pn[i] / 1000);
        if (it != dict.cend())
          dno[i] = 1;
        else
          dno[i] = 0;
      }
      if (dno[i] == 0) {
        const auto it = dict.find(token[i], pn[i]);
        if (it != dict.cend())
          dno[i] = 1;
        else
          dno[i] = 0;
      }
    }

    pn += M;
    dno += M;
    N -= M;
  }
}

void DnoMapping::getDNOs(size_t N, const uint64_t *pn, uint64_t *dno) const {
  data_->getDNOs(N, pn, dno);
}

uint64_t DnoMapping::getDNO(uint64_t pn) const {
  uint64_t dno;
  getDNOs(1, &pn, &dno);
  return dno;
}

DnoMapping::Builder::Builder()
  : data_(std::make_unique<Data>())
{}

DnoMapping::Builder::~Builder() noexcept = default;

void DnoMapping::Builder::sizeHint(size_t numRecords) {
  data_->dict.reserve(numRecords);
}

void DnoMapping::Builder::setMetadata(const folly::dynamic &meta) {
  data_->meta = meta;
}

DnoMapping::Builder& DnoMapping::Builder::addRow(uint64_t pn, std::string dnotype, uint64_t dno) {
  if (dnotype == std::string("dno")) {
    if (data_->dict.count(pn))
      throw std::runtime_error("DnoMapping::Builder: duplicate key");
    data_->dict.emplace(pn, dno);
  } else if (dnotype == std::string("dno_npa")) {
    if (data_->dict_npa.count(pn))
      throw std::runtime_error("DnoMapping::Builder: duplicate key");
    data_->dict_npa.emplace(pn, dno);
  } else if (dnotype == std::string("dno_npa_nxx")) {
    if (data_->dict_npa_nxx.count(pn))
      throw std::runtime_error("DnoMapping::Builder: duplicate key");
    data_->dict_npa_nxx.emplace(pn, dno);
  } else if (dnotype == std::string("dno_npa_nxx_x")) {
    if (data_->dict_npa_nxx_x.count(pn))
      throw std::runtime_error("DnoMapping::Builder: duplicate key");
    data_->dict_npa_nxx_x.emplace(pn, dno);
  }
    
  return *this;
}

std::string DnoMapping::Builder::deleteCharacter(const std::string& input, char character) {
    std::string result;
    for (char c : input) {
        if (c != character) {
            result += c;
        }
    }
    return result;
}

void DnoMapping::Builder::fromCSV(std::istream &in, std::string dnotype, size_t &line, size_t limit) {
  std::string linebuf;
  std::string number;

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
      number = deleteCharacter(parts[0], '-');
      uint64_t pn = std::stoll(number);
      addRow(pn, dnotype, 1); // Only need phone number
    }
    else
      throw std::runtime_error("bad number of columns");
  }

}

void DnoMapping::Data::build() {
  
}

DnoMapping DnoMapping::Builder::build() {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();
  return DnoMapping(std::move(data));
}

void DnoMapping::Builder::commit(std::atomic<Data*> &global) {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();

  if (Data *veteran = global.exchange(data.release()))
    veteran->retire();
  LOG(INFO) << "Database updated:";
}

DnoMapping::DnoMapping(std::unique_ptr<Data> data) {
  CHECK(FLAGS_dno_f14map_prefetch > 0);
  holder_.reset(data.get());
  data->retire();
  data_ = data.release();
}

DnoMapping::DnoMapping(std::atomic<Data*> &global)
  : data_(holder_.get_protected(global))
{
  CHECK(FLAGS_dno_f14map_prefetch > 0);
}

DnoMapping::DnoMapping(DnoMapping&& rhs) noexcept = default;
DnoMapping::~DnoMapping() noexcept = default;

void DnoMapping::printMetadata() {
  if (!data_)
    return;
  LOG(INFO) << "Current mapping info:";
  for (auto kv : data_->meta.items())
    LOG(INFO) << "  " << kv.first.asString()
                  << ": " << folly::toJson(kv.second);
}

