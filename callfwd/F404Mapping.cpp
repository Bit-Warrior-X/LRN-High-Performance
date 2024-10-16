#include "F404Mapping.h"
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
DEFINE_uint32(F404_f14map_prefetch, 16, "Maximum number of keys to prefetch");

struct PhoneList {
  uint64_t phone : 34, next : 30;
};
static constexpr uint64_t MAXROWS = (1 << 30) - 1;
static_assert(sizeof(PhoneList) == 8, "");

class F404Mapping::Data : public folly::hazptr_obj_base<F404Mapping::Data> {
 public:
  void getF404s(size_t N, const uint64_t *pn, F404Data *F404) const;
  std::unique_ptr<Cursor> visitRows() const;
  void build();
  ~Data() noexcept;

  // metadata
  folly::dynamic meta;
  // pn->dn mapping
  folly::F14ValueMap<uint64_t, F404Data> dict;
  // pn column joined with sorted F404 column
  std::vector<PhoneList> pnColumn;
  // unique-sorted F404 column joined with pn
  std::vector<PhoneList> F404Index;
};

F404Mapping::Data::~Data() noexcept {
  LOG_IF(INFO, pnColumn.size() > 0) << "Reclaiming memory";
}

class F404Mapping::Cursor {
 public:
  virtual ~Cursor() = default;

  bool hasRow() const noexcept { return size_ != 0; }
  uint64_t currentPN() const noexcept { return pn_[pos_]; }
  F404Data currentF404() const noexcept { return F404_[pos_]; }
  void prefetch(const Data *data) noexcept;
  void advance(const Data *data) noexcept;
  virtual void refill() = 0;

 protected:
  std::array<uint64_t, 8> pn_;
  std::array<F404Data, 8> F404_;
  unsigned size_;
  unsigned pos_;
};

void F404Mapping::Data::getF404s(size_t N, const uint64_t *pn, F404Data *F404) const {
  folly::small_vector<folly::F14HashToken, 1> token;
  token.resize(std::min<size_t>(N, FLAGS_F404_f14map_prefetch));
  while (N > 0) {
    size_t M = std::min<size_t>(N, FLAGS_F404_f14map_prefetch);
  
    // Compute hash and prefetch buckets into CPU cache
    for (size_t i = 0; i < M; ++i) {
      token[i] = dict.prehash(pn[i]);
    }

    // Fill output vector
    for (size_t i = 0; i < M; ++i) {
      const auto it = dict.find(token[i], pn[i]);
      if (it != dict.cend()) {
        F404Data data = it->second;
        F404[i].pn = data.pn;
        F404[i].first_F404_on = data.first_F404_on;
        F404[i].last_F404_on = data.last_F404_on;
      }
      else
        F404[i].pn = 0;
    }
    
    pn += M;
    F404 += M;
    N -= M;
  }
}

void F404Mapping::getF404s(size_t N, const uint64_t *pn, F404Data *F404) const {
  data_->getF404s(N, pn, F404);
}

F404Data F404Mapping::getF404(uint64_t pn) const {
  F404Data F404;
  getF404s(1, &pn, &F404);
  return F404;
}

F404Mapping::Builder::Builder()
  : data_(std::make_unique<Data>())
{}

F404Mapping::Builder::~Builder() noexcept = default;

void F404Mapping::Builder::sizeHint(size_t numRecords) {
  data_->pnColumn.reserve(numRecords);
  data_->F404Index.reserve(numRecords);
  data_->dict.reserve(numRecords);
}

void F404Mapping::Builder::setMetadata(const folly::dynamic &meta) {
  data_->meta = meta;
}

F404Mapping::Builder& F404Mapping::Builder::addRow(std::vector<std::string> rowbuf) {
  uint64_t pn;

  std::string phoneNumberStr = rowbuf[0];
  phoneNumberStr = phoneNumberStr.substr(1, 10); //19169954938,2021-02-09 04:11:39,2021-07-03 14:53:37,\N
  pn = std::stoll(phoneNumberStr);

  //LOG(INFO) << "Add new row " << pn;
  //LOG(INFO) << "F404_key method dict is " << pn;  
  
  if (data_->dict.count(pn)) {
    //throw std::runtime_error("F404Mapping::Builder: duplicate key");
    return *this;
  }

  if (data_->pnColumn.size() >= MAXROWS) {
    //throw std::runtime_error("F404Mapping::Builder: too much rows");
    return *this;
  }

  F404Data data;
  data.pn = pn;
  data.first_F404_on = rowbuf[1];
  data.last_F404_on = rowbuf[2];
  
  data_->dict.emplace(pn, data);
  data_->pnColumn.push_back(PhoneList{pn, MAXROWS});
  data_->F404Index.push_back(PhoneList{pn, MAXROWS});

  return *this;
}

std::string F404Mapping::Builder::deleteCharacter(const std::string& input, char character) {
    std::string result;
    for (char c : input) {
        if (c != character) {
            result += c;
        }
    }
    return result;
}
void F404Mapping::Builder::fromCSV(std::istream &in, size_t &line, size_t limit) {
  std::string linebuf;
  std::string number;

  //LOG(INFO) << "F404Mapping fromCSV : ";

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

    if (linebuf[0] != '1')
      continue;

    //LOG(INFO) << "part size is " << parts.size();
    if (parts.size() >= 3) {
      addRow(parts); 
    }
    else
      throw std::runtime_error("bad number of columns");
  }
}

void F404Mapping::Data::build() {
  size_t N = pnColumn.size();

  // Connect F404Index_ with pnColumn_ before shuffling
  for (size_t i = 0; i < N; ++i)
    F404Index[i].next = i;

  static auto cmp = [](const PhoneList &lhs, const PhoneList &rhs) {
    if (lhs.phone == rhs.phone)
      return lhs.next < rhs.next;
    return lhs.phone < rhs.phone;
  };

#if HAVE_STD_PARALLEL
  std::stable_sort(std::execution::par_unseq, F404Index.begin(), F404Index.end(), cmp);
#else
  std::stable_sort(F404Index.begin(), F404Index.end(), cmp);
#endif

  // Wire pnColumn_ list by target
  for (size_t i = 0; i + 1 < N; ++i)
    pnColumn[F404Index[i].next].next = F404Index[i+1].next;

  static auto equal = [](const PhoneList &lhs, const PhoneList &rhs) {
    return lhs.phone == rhs.phone;
  };
  auto last = std::unique(F404Index.begin(), F404Index.end(), equal);
  F404Index.erase(last, F404Index.end());
  F404Index.shrink_to_fit();
}

F404Mapping F404Mapping::Builder::build() {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();
  return F404Mapping(std::move(data));
}

void F404Mapping::Builder::commit(std::atomic<Data*> &global) {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();

  size_t pn_count = data->pnColumn.size();
  size_t F404_count = data->F404Index.size();
  if (Data *veteran = global.exchange(data.release()))
    veteran->retire();
  LOG(INFO) << "Database updated: PNs=" << pn_count << " F404s=" << F404_count;
}

F404Mapping::F404Mapping(std::unique_ptr<Data> data) {
  CHECK(FLAGS_F404_f14map_prefetch > 0);
  holder_.reset(data.get());
  data->retire();
  data_ = data.release();
}

F404Mapping::F404Mapping(std::atomic<Data*> &global)
  : data_(holder_.get_protected(global))
{
  CHECK(FLAGS_F404_f14map_prefetch > 0);
}

F404Mapping::F404Mapping(F404Mapping&& rhs) noexcept = default;
F404Mapping::~F404Mapping() noexcept = default;

void F404Mapping::printMetadata() {
  if (!data_)
    return;
  LOG(INFO) << "Current mapping info:";
  for (auto kv : data_->meta.items())
    LOG(INFO) << "  " << kv.first.asString()
                  << ": " << folly::toJson(kv.second);
}

size_t F404Mapping::size() const noexcept {
  return data_->pnColumn.size();
}

