#include "GeoMapping.h"
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
DEFINE_uint32(geo_f14map_prefetch, 16, "Maximum number of keys to prefetch");

struct PhoneList {
  uint64_t phone : 34, next : 30;
};
static constexpr uint64_t MAXROWS = (1 << 30) - 1;
static_assert(sizeof(PhoneList) == 8, "");

class GeoMapping::Data : public folly::hazptr_obj_base<GeoMapping::Data> {
 public:
  void getGeos(size_t N, const uint64_t *pn, GeoData *geo) const;
  std::unique_ptr<Cursor> visitRows() const;
  void build();
  ~Data() noexcept;

  // metadata
  folly::dynamic meta;
  // pn->dn mapping
  folly::F14ValueMap<uint64_t, GeoData> dict;
  // pn column joined with sorted geo column
  std::vector<PhoneList> pnColumn;
  // unique-sorted geo column joined with pn
  std::vector<PhoneList> geoIndex;
};

GeoMapping::Data::~Data() noexcept {
  LOG_IF(INFO, pnColumn.size() > 0) << "Reclaiming memory";
}

class GeoMapping::Cursor {
 public:
  virtual ~Cursor() = default;

  bool hasRow() const noexcept { return size_ != 0; }
  uint64_t currentPN() const noexcept { return pn_[pos_]; }
  GeoData currentgeo() const noexcept { return geo_[pos_]; }
  void prefetch(const Data *data) noexcept;
  void advance(const Data *data) noexcept;
  virtual void refill() = 0;

 protected:
  std::array<uint64_t, 8> pn_;
  std::array<GeoData, 8> geo_;
  unsigned size_;
  unsigned pos_;
};

void GeoMapping::Data::getGeos(size_t N, const uint64_t *pn, GeoData *geo) const {
  folly::small_vector<folly::F14HashToken, 1> token;
  token.resize(std::min<size_t>(N, FLAGS_geo_f14map_prefetch));
  while (N > 0) {
    size_t M = std::min<size_t>(N, FLAGS_geo_f14map_prefetch);
  
    // Compute hash and prefetch buckets into CPU cache
    for (size_t i = 0; i < M; ++i) {
      uint64_t npanxx = pn[i] / 10000;
      token[i] = dict.prehash(npanxx);
    }

    // Fill output vector
    for (size_t i = 0; i < M; ++i) {
      uint64_t npanxx = pn[i] / 10000;
      const auto it = dict.find(token[i], npanxx);
      if (it != dict.cend()) {
        GeoData data = it->second;
        geo[i].npanxx = data.npanxx;
        geo[i].zipcode = data.zipcode;
        geo[i].county = data.county;
        geo[i].city = data.city;
        geo[i].latitude = data.latitude;
        geo[i].longitude = data.longitude;
        geo[i].timezone = data.timezone;
      }
      else
        geo[i].npanxx = 0;
    }

    pn += M;
    geo += M;
    N -= M;
  }
}

void GeoMapping::getGeos(size_t N, const uint64_t *pn, GeoData *geo) const {
  data_->getGeos(N, pn, geo);
}

GeoData GeoMapping::getGeo(uint64_t pn) const {
  GeoData geo;
  getGeos(1, &pn, &geo);
  return geo;
}

GeoMapping::Builder::Builder()
  : data_(std::make_unique<Data>())
{}

GeoMapping::Builder::~Builder() noexcept = default;

void GeoMapping::Builder::sizeHint(size_t numRecords) {
  data_->pnColumn.reserve(numRecords);
  data_->geoIndex.reserve(numRecords);
  data_->dict.reserve(numRecords);
}

void GeoMapping::Builder::setMetadata(const folly::dynamic &meta) {
  data_->meta = meta;
}

GeoMapping::Builder& GeoMapping::Builder::addRow(std::vector<std::string> rowbuf) {
  uint64_t npanxx;

  //LOG(INFO) << "Add new row " << rowbuf[0];

  std::string phoneNumberStr = rowbuf[0];
  npanxx = std::stoll(phoneNumberStr);
  
  //LOG(INFO) << "geo_key method dict is " << npanxx;  
  if (data_->dict.count(npanxx))
    throw std::runtime_error("GeoMapping::Builder: duplicate key");
  if (data_->pnColumn.size() >= MAXROWS)
    throw std::runtime_error("GeoMapping::Builder: too much rows");

  std::string zipcode;
  std::string county;
  std::string city;
  std::string latitude;
  std::string longitude;
  std::string timezone;

  GeoData data;
  data.npanxx = npanxx;
  data.zipcode = rowbuf[1];
  data.county = rowbuf[10];
  data.city = rowbuf[6];
  data.latitude = rowbuf[9];
  data.longitude = rowbuf[11];
  data.timezone = rowbuf[19];

  data_->dict.emplace(npanxx, data);
  data_->pnColumn.push_back(PhoneList{npanxx, MAXROWS});
  data_->geoIndex.push_back(PhoneList{npanxx, MAXROWS});
  return *this;
}

std::string GeoMapping::Builder::deleteCharacter(const std::string& input, char character) {
    std::string result;
    for (char c : input) {
        if (c != character) {
            result += c;
        }
    }
    return result;
}
void GeoMapping::Builder::fromCSV(std::istream &in, size_t &line, size_t limit) {
  std::string linebuf;
  std::string number;

  //LOG(INFO) << "GeoMapping fromCSV : ";

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

    //LOG(INFO) << "part size is " << parts.size();
    if (parts.size() >= 19) {
      addRow(parts); 
    }
    else
      throw std::runtime_error("bad number of columns");
  }
}

void GeoMapping::Data::build() {
  size_t N = pnColumn.size();

  // Connect geoIndex_ with pnColumn_ before shuffling
  for (size_t i = 0; i < N; ++i)
    geoIndex[i].next = i;

  static auto cmp = [](const PhoneList &lhs, const PhoneList &rhs) {
    if (lhs.phone == rhs.phone)
      return lhs.next < rhs.next;
    return lhs.phone < rhs.phone;
  };

#if HAVE_STD_PARALLEL
  std::stable_sort(std::execution::par_unseq, geoIndex.begin(), geoIndex.end(), cmp);
#else
  std::stable_sort(geoIndex.begin(), geoIndex.end(), cmp);
#endif

  // Wire pnColumn_ list by target
  for (size_t i = 0; i + 1 < N; ++i)
    pnColumn[geoIndex[i].next].next = geoIndex[i+1].next;

  static auto equal = [](const PhoneList &lhs, const PhoneList &rhs) {
    return lhs.phone == rhs.phone;
  };
  auto last = std::unique(geoIndex.begin(), geoIndex.end(), equal);
  geoIndex.erase(last, geoIndex.end());
  geoIndex.shrink_to_fit();
}

GeoMapping GeoMapping::Builder::build() {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();
  return GeoMapping(std::move(data));
}

void GeoMapping::Builder::commit(std::atomic<Data*> &global) {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();

  size_t pn_count = data->pnColumn.size();
  size_t geo_count = data->geoIndex.size();
  if (Data *veteran = global.exchange(data.release()))
    veteran->retire();
  LOG(INFO) << "Database updated: PNs=" << pn_count << " geos=" << geo_count;
}

GeoMapping::GeoMapping(std::unique_ptr<Data> data) {
  CHECK(FLAGS_geo_f14map_prefetch > 0);
  holder_.reset(data.get());
  data->retire();
  data_ = data.release();
}

GeoMapping::GeoMapping(std::atomic<Data*> &global)
  : data_(holder_.get_protected(global))
{
  CHECK(FLAGS_geo_f14map_prefetch > 0);
}

GeoMapping::GeoMapping(GeoMapping&& rhs) noexcept = default;
GeoMapping::~GeoMapping() noexcept = default;

void GeoMapping::printMetadata() {
  if (!data_)
    return;
  LOG(INFO) << "Current mapping info:";
  for (auto kv : data_->meta.items())
    LOG(INFO) << "  " << kv.first.asString()
                  << ": " << folly::toJson(kv.second);
}

size_t GeoMapping::size() const noexcept {
  return data_->pnColumn.size();
}

