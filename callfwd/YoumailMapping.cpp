#include "YoumailMapping.h"
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
DEFINE_uint32(youmail_f14map_prefetch, 16, "Maximum number of keys to prefetch");

struct PhoneList {
  uint64_t phone : 34, next : 30;
};
static constexpr uint64_t MAXROWS = (1 << 30) - 1;
static_assert(sizeof(PhoneList) == 8, "");

class YoumailMapping::Data : public folly::hazptr_obj_base<YoumailMapping::Data> {
 public:
  void getYoumails(size_t N, const uint64_t *pn, YoumailData *youmail) const;
  std::unique_ptr<Cursor> visitRows() const;
  void build();
  ~Data() noexcept;

  // metadata
  folly::dynamic meta;
  // pn->dn mapping
  folly::F14ValueMap<uint64_t, YoumailData> dict;
  // pn column joined with sorted youmail column
  std::vector<PhoneList> pnColumn;
  // unique-sorted youmail column joined with pn
  std::vector<PhoneList> youmailIndex;
};

YoumailMapping::Data::~Data() noexcept {
  LOG_IF(INFO, pnColumn.size() > 0) << "Reclaiming memory";
}

class YoumailMapping::Cursor {
 public:
  virtual ~Cursor() = default;

  bool hasRow() const noexcept { return size_ != 0; }
  uint64_t currentPN() const noexcept { return pn_[pos_]; }
  YoumailData currentYoumail() const noexcept { return youmail_[pos_]; }
  void prefetch(const Data *data) noexcept;
  void advance(const Data *data) noexcept;
  virtual void refill() = 0;

 protected:
  std::array<uint64_t, 8> pn_;
  std::array<YoumailData, 8> youmail_;
  unsigned size_;
  unsigned pos_;
};

void YoumailMapping::Data::getYoumails(size_t N, const uint64_t *pn, YoumailData *youmail) const {
  folly::small_vector<folly::F14HashToken, 1> token;
  token.resize(std::min<size_t>(N, FLAGS_youmail_f14map_prefetch));
  while (N > 0) {
    size_t M = std::min<size_t>(N, FLAGS_youmail_f14map_prefetch);
  
    // Compute hash and prefetch buckets into CPU cache
    for (size_t i = 0; i < M; ++i) {
      token[i] = dict.prehash(pn[i]);
    }

    // Fill output vector
    for (size_t i = 0; i < M; ++i) {
      const auto it = dict.find(token[i], pn[i]);
      if (it != dict.cend()) {
        YoumailData data = it->second;
        youmail[i].pn = data.pn;
        youmail[i].sapmscore = data.sapmscore;
        youmail[i].fraudprobability = data.fraudprobability;
        youmail[i].unlawful = data.unlawful;
        youmail[i].tcpafraud = data.tcpafraud;
      }
      else
        youmail[i].pn = 0;
    }

    pn += M;
    youmail += M;
    N -= M;
  }
}

void YoumailMapping::getYoumails(size_t N, const uint64_t *pn, YoumailData *youmail) const {
  data_->getYoumails(N, pn, youmail);
}

YoumailData YoumailMapping::getYoumail(uint64_t pn) const {
  YoumailData youmail;
  getYoumails(1, &pn, &youmail);
  return youmail;
}

YoumailMapping::Builder::Builder()
  : data_(std::make_unique<Data>())
{}

YoumailMapping::Builder::~Builder() noexcept = default;

void YoumailMapping::Builder::sizeHint(size_t numRecords) {
  data_->pnColumn.reserve(numRecords);
  data_->youmailIndex.reserve(numRecords);
  data_->dict.reserve(numRecords);
}

void YoumailMapping::Builder::setMetadata(const folly::dynamic &meta) {
  data_->meta = meta;
}

YoumailMapping::Builder& YoumailMapping::Builder::addRow(std::vector<std::string> rowbuf) {
  uint64_t pn;

  //LOG(INFO) << "Add new row " << rowbuf[0];

  std::string phoneNumberStr = rowbuf[0];
  std::string toRemove = "+1";

  size_t found = phoneNumberStr.find(toRemove);
  while (found != std::string::npos) {
      phoneNumberStr.erase(found, toRemove.length());
      found = phoneNumberStr.find(toRemove, found);
      break; // remove first +1
  }

  pn = std::stoll(phoneNumberStr);
  
  //LOG(INFO) << "youmail_key method dict is " << pn;  
  if (data_->dict.count(pn))
    throw std::runtime_error("YoumailMapping::Builder: duplicate key");
  if (data_->pnColumn.size() >= MAXROWS)
    throw std::runtime_error("YoumailMapping::Builder: too much rows");

  YoumailData data;
  data.pn = pn;
  data.sapmscore = rowbuf[1];
  data.fraudprobability = rowbuf[2];
  data.unlawful = rowbuf[3];
  if (rowbuf.size() == 4)
    data.tcpafraud = std::string("");
  else
    data.tcpafraud = rowbuf[4];

  data_->dict.emplace(pn, data);
  data_->pnColumn.push_back(PhoneList{pn, MAXROWS});
  data_->youmailIndex.push_back(PhoneList{pn, MAXROWS});
  return *this;
}

std::string YoumailMapping::Builder::deleteCharacter(const std::string& input, char character) {
    std::string result;
    for (char c : input) {
        if (c != character) {
            result += c;
        }
    }
    return result;
}
void YoumailMapping::Builder::fromCSV(std::istream &in, size_t &line, size_t limit) {
  std::string linebuf;
  std::string number;

  //LOG(INFO) << "YoumailMapping fromCSV : ";

  for (limit += line; line < limit; ++line) {
    if (in.peek() == EOF)
      break;
    std::getline(in, linebuf);

    if (linebuf[0] != '+')
      continue;

    //LOG(INFO) << "pass first test";
    std::vector<std::string> parts;
    std::stringstream ss(linebuf);
    std::string part;

    while (std::getline(ss, part, ',')) {
        parts.push_back(part);
    }

    //LOG(INFO) << "part size is " << parts.size();
    if (parts.size() == 5) {
      addRow(parts); 
    } else if (parts.size() == 4 && linebuf.back() == ',') { // +10000000039,ALMOST_CERTAINLY,,,
      addRow(parts);
    }
    else
      throw std::runtime_error("bad number of columns");
  }
}

void YoumailMapping::Data::build() {
  size_t N = pnColumn.size();

  // Connect youmailIndex_ with pnColumn_ before shuffling
  for (size_t i = 0; i < N; ++i)
    youmailIndex[i].next = i;

  static auto cmp = [](const PhoneList &lhs, const PhoneList &rhs) {
    if (lhs.phone == rhs.phone)
      return lhs.next < rhs.next;
    return lhs.phone < rhs.phone;
  };

#if HAVE_STD_PARALLEL
  std::stable_sort(std::execution::par_unseq, youmailIndex.begin(), youmailIndex.end(), cmp);
#else
  std::stable_sort(youmailIndex.begin(), youmailIndex.end(), cmp);
#endif

  // Wire pnColumn_ list by target
  for (size_t i = 0; i + 1 < N; ++i)
    pnColumn[youmailIndex[i].next].next = youmailIndex[i+1].next;

  static auto equal = [](const PhoneList &lhs, const PhoneList &rhs) {
    return lhs.phone == rhs.phone;
  };
  auto last = std::unique(youmailIndex.begin(), youmailIndex.end(), equal);
  youmailIndex.erase(last, youmailIndex.end());
  youmailIndex.shrink_to_fit();
}

YoumailMapping YoumailMapping::Builder::build() {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();
  return YoumailMapping(std::move(data));
}

void YoumailMapping::Builder::commit(std::atomic<Data*> &global) {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();

  size_t pn_count = data->pnColumn.size();
  size_t youmail_count = data->youmailIndex.size();
  if (Data *veteran = global.exchange(data.release()))
    veteran->retire();
  LOG(INFO) << "Database updated: PNs=" << pn_count << " youmails=" << youmail_count;
}

YoumailMapping::YoumailMapping(std::unique_ptr<Data> data) {
  CHECK(FLAGS_youmail_f14map_prefetch > 0);
  holder_.reset(data.get());
  data->retire();
  data_ = data.release();
}

YoumailMapping::YoumailMapping(std::atomic<Data*> &global)
  : data_(holder_.get_protected(global))
{
  CHECK(FLAGS_youmail_f14map_prefetch > 0);
}

YoumailMapping::YoumailMapping(YoumailMapping&& rhs) noexcept = default;
YoumailMapping::~YoumailMapping() noexcept = default;

void YoumailMapping::printMetadata() {
  if (!data_)
    return;
  LOG(INFO) << "Current mapping info:";
  for (auto kv : data_->meta.items())
    LOG(INFO) << "  " << kv.first.asString()
                  << ": " << folly::toJson(kv.second);
}

size_t YoumailMapping::size() const noexcept {
  return data_->pnColumn.size();
}

