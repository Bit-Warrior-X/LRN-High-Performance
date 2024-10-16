#include "F606Mapping.h"
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
DEFINE_uint32(F606_f14map_prefetch, 16, "Maximum number of keys to prefetch");

struct PhoneList {
  uint64_t phone : 34, next : 30;
};
static constexpr uint64_t MAXROWS = (1 << 30) - 1;
static_assert(sizeof(PhoneList) == 8, "");

class F606Mapping::Data : public folly::hazptr_obj_base<F606Mapping::Data> {
 public:
  void getF606s(size_t N, const uint64_t *pn, F606Data *F606) const;
  std::unique_ptr<Cursor> visitRows() const;
  void build();
  ~Data() noexcept;

  // metadata
  folly::dynamic meta;
  // pn->dn mapping
  folly::F14ValueMap<uint64_t, F606Data> dict;
  // pn column joined with sorted F606 column
  std::vector<PhoneList> pnColumn;
  // unique-sorted F606 column joined with pn
  std::vector<PhoneList> F606Index;
};

F606Mapping::Data::~Data() noexcept {
  LOG_IF(INFO, pnColumn.size() > 0) << "Reclaiming memory";
}

class F606Mapping::Cursor {
 public:
  virtual ~Cursor() = default;

  bool hasRow() const noexcept { return size_ != 0; }
  uint64_t currentPN() const noexcept { return pn_[pos_]; }
  F606Data currentF606() const noexcept { return F606_[pos_]; }
  void prefetch(const Data *data) noexcept;
  void advance(const Data *data) noexcept;
  virtual void refill() = 0;

 protected:
  std::array<uint64_t, 8> pn_;
  std::array<F606Data, 8> F606_;
  unsigned size_;
  unsigned pos_;
};

void F606Mapping::Data::getF606s(size_t N, const uint64_t *pn, F606Data *F606) const {
  folly::small_vector<folly::F14HashToken, 1> token;
  token.resize(std::min<size_t>(N, FLAGS_F606_f14map_prefetch));
  while (N > 0) {
    size_t M = std::min<size_t>(N, FLAGS_F606_f14map_prefetch);
  
    // Compute hash and prefetch buckets into CPU cache
    for (size_t i = 0; i < M; ++i) {
      token[i] = dict.prehash(pn[i]);
    }

    // Fill output vector
    for (size_t i = 0; i < M; ++i) {
      const auto it = dict.find(token[i], pn[i]);
      if (it != dict.cend()) {
        F606Data data = it->second;
        F606[i].pn = data.pn;
        F606[i].first_F606_on = data.first_F606_on;
        F606[i].last_F606_on = data.last_F606_on;
      }
      else
        F606[i].pn = 0;
    }
    
    pn += M;
    F606 += M;
    N -= M;
  }
}

void F606Mapping::getF606s(size_t N, const uint64_t *pn, F606Data *F606) const {
  data_->getF606s(N, pn, F606);
}

F606Data F606Mapping::getF606(uint64_t pn) const {
  F606Data F606;
  getF606s(1, &pn, &F606);
  return F606;
}

F606Mapping::Builder::Builder()
  : data_(std::make_unique<Data>())
{}

F606Mapping::Builder::~Builder() noexcept = default;

void F606Mapping::Builder::sizeHint(size_t numRecords) {
  data_->pnColumn.reserve(numRecords);
  data_->F606Index.reserve(numRecords);
  data_->dict.reserve(numRecords);
}

void F606Mapping::Builder::setMetadata(const folly::dynamic &meta) {
  data_->meta = meta;
}

F606Mapping::Builder& F606Mapping::Builder::addRow(std::vector<std::string> rowbuf) {
  uint64_t pn;

  //LOG(INFO) << "Add new row " << rowbuf[0];

  std::string phoneNumberStr = rowbuf[0];
  phoneNumberStr = phoneNumberStr.substr(1, 10); //19169954938,2021-02-09 04:11:39,2021-07-03 14:53:37,\N
  pn = std::stoll(phoneNumberStr);
  
  //LOG(INFO) << "F606_key method dict is " << pn;  
  if (data_->dict.count(pn)) {
    //throw std::runtime_error("F404Mapping::Builder: duplicate key");
    return *this;
  }

  if (data_->pnColumn.size() >= MAXROWS) {
    //throw std::runtime_error("F404Mapping::Builder: too much rows");
    return *this;
  }

  F606Data data;
  data.pn = pn;
  data.first_F606_on = rowbuf[1];
  data.last_F606_on = rowbuf[2];
  
  data_->dict.emplace(pn, data);
  data_->pnColumn.push_back(PhoneList{pn, MAXROWS});
  data_->F606Index.push_back(PhoneList{pn, MAXROWS});
  return *this;
}

std::string F606Mapping::Builder::deleteCharacter(const std::string& input, char character) {
    std::string result;
    for (char c : input) {
        if (c != character) {
            result += c;
        }
    }
    return result;
}
void F606Mapping::Builder::fromCSV(std::istream &in, size_t &line, size_t limit) {
  std::string linebuf;
  std::string number;

  //LOG(INFO) << "F606Mapping fromCSV : ";

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

void F606Mapping::Data::build() {
  size_t N = pnColumn.size();

  // Connect F606Index_ with pnColumn_ before shuffling
  for (size_t i = 0; i < N; ++i)
    F606Index[i].next = i;

  static auto cmp = [](const PhoneList &lhs, const PhoneList &rhs) {
    if (lhs.phone == rhs.phone)
      return lhs.next < rhs.next;
    return lhs.phone < rhs.phone;
  };

#if HAVE_STD_PARALLEL
  std::stable_sort(std::execution::par_unseq, F606Index.begin(), F606Index.end(), cmp);
#else
  std::stable_sort(F606Index.begin(), F606Index.end(), cmp);
#endif

  // Wire pnColumn_ list by target
  for (size_t i = 0; i + 1 < N; ++i)
    pnColumn[F606Index[i].next].next = F606Index[i+1].next;

  static auto equal = [](const PhoneList &lhs, const PhoneList &rhs) {
    return lhs.phone == rhs.phone;
  };
  auto last = std::unique(F606Index.begin(), F606Index.end(), equal);
  F606Index.erase(last, F606Index.end());
  F606Index.shrink_to_fit();
}

F606Mapping F606Mapping::Builder::build() {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();
  return F606Mapping(std::move(data));
}

void F606Mapping::Builder::commit(std::atomic<Data*> &global) {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();

  size_t pn_count = data->pnColumn.size();
  size_t F606_count = data->F606Index.size();
  if (Data *veteran = global.exchange(data.release()))
    veteran->retire();
  LOG(INFO) << "Database updated: PNs=" << pn_count << " F606s=" << F606_count;
}

F606Mapping::F606Mapping(std::unique_ptr<Data> data) {
  CHECK(FLAGS_F606_f14map_prefetch > 0);
  holder_.reset(data.get());
  data->retire();
  data_ = data.release();
}

F606Mapping::F606Mapping(std::atomic<Data*> &global)
  : data_(holder_.get_protected(global))
{
  CHECK(FLAGS_F606_f14map_prefetch > 0);
}

F606Mapping::F606Mapping(F606Mapping&& rhs) noexcept = default;
F606Mapping::~F606Mapping() noexcept = default;

void F606Mapping::printMetadata() {
  if (!data_)
    return;
  LOG(INFO) << "Current mapping info:";
  for (auto kv : data_->meta.items())
    LOG(INFO) << "  " << kv.first.asString()
                  << ": " << folly::toJson(kv.second);
}

size_t F606Mapping::size() const noexcept {
  return data_->pnColumn.size();
}

