#include <functional>
#include <gflags/gflags.h>
#include <folly/Likely.h>
#include <folly/Range.h>
#include <folly/Conv.h>
#include <folly/Format.h>
#include <folly/small_vector.h>
#include <proxygen/lib/http/HTTPCommonHeaders.h>
#include <proxygen/lib/http/HTTPMethod.h>
#include <proxygen/lib/http/RFC2616.h>
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/httpserver/filters/DirectResponseHandler.h>

#include "PhoneMapping.h"
#include "DncMapping.h"
#include "DnoMapping.h"
#include "TollFreeMapping.h"
#include "LergMapping.h"
#include "YoumailMapping.h"
#include "GeoMapping.h"
#include "FtcMapping.h"
#include "F404Mapping.h"
#include "F606Mapping.h"
#include "AccessLog.h"

using namespace proxygen;
using folly::StringPiece;

DEFINE_uint32(max_query_length, 32768,
              "Maximum length of POST x-www-form-urlencoded body");


bool isJsonRequested(StringPiece accept) {
  RFC2616::TokenPairVec acceptTok;
  RFC2616::parseQvalues(accept, acceptTok);
  return acceptTok.size() > 0 && acceptTok[0].first == "application/json";
}

class TargetHandler final : public RequestHandler {
 public:
  void onRequest(std::unique_ptr<HTTPMessage> req) noexcept override {
    using namespace std::placeholders;
    req->getHeaders()
      .forEachWithCode(std::bind(&TargetHandler::sanitizeHeader,
                                 this, _1, _2, _3));

    if (req->getMethod() == HTTPMethod::GET) {
      needBody_ = false;
      onQueryString(req->getQueryStringAsStringPiece());
      onQueryComplete();
      return;
    }

    if (req->getMethod() == HTTPMethod::POST) {
      if (needBody_)
        return;
    }

    ResponseBuilder(downstream_)
      .status(400, "Bad Request")
      .sendWithEOM();
    needBody_ = false;
  }

  void sanitizeHeader(HTTPHeaderCode code, const std::string& name,
                      const std::string& value) noexcept {
    switch (code) {
    case HTTP_HEADER_CONTENT_LENGTH:
      if (folly::to<size_t>(value) > FLAGS_max_query_length)
        needBody_ = false;
      break;
    case HTTP_HEADER_CONTENT_TYPE:
      if (value != "application/x-www-form-urlencoded")
        needBody_ = false;
      break;
    case HTTP_HEADER_ACCEPT:
      json_ = isJsonRequested(value);
      break;
    default:
      break;
    }
  }

  void onQueryComplete() noexcept {
    size_t N = pn_.size();
    std::string record;
    bool dnoAvailable = false;
    bool dncAvailable = false;
    bool tollfreeAvailable = false;
    bool lergAvailable = false;
    bool youmailAvailable = false;
    bool geoAvailable = false;
    bool ftcAvailable = false;
    bool f404Available = false;
    bool f606Available = false;

    if (LIKELY(DncMapping::isAvailable())) {
      dncAvailable = true;
    } else {
      dncAvailable = false;
    }

    if (LIKELY(DnoMapping::isAvailable())) {
      dnoAvailable = true;
    } else {
      dnoAvailable = false;
    }

    if (LIKELY(TollFreeMapping::isAvailable())) {
      tollfreeAvailable = true;
    } else {
      tollfreeAvailable = false;
    }

    if (LIKELY(LergMapping::isAvailable())) {
      lergAvailable = true;
    } else {
      lergAvailable = false;
    }

    if (LIKELY(YoumailMapping::isAvailable())) {
      youmailAvailable = true;
    } else {
      youmailAvailable = false;
    }

    if (LIKELY(GeoMapping::isAvailable())) {
      geoAvailable = true;
    } else {
      geoAvailable = false;
    }

    if (LIKELY(FtcMapping::isAvailable())) {
      ftcAvailable = true;
    } else {
      ftcAvailable = false;
    }

    if (LIKELY(F404Mapping::isAvailable())) {
      f404Available = true;
    } else {
      f404Available = false;
    }

    if (LIKELY(F606Mapping::isAvailable())) {
      f606Available = true;
    } else {
      f606Available = false;
    }

    us_rn_.resize(N);
    ca_rn_.resize(N);
    if (dncAvailable)
      us_dnc_.resize(N);
    if (dnoAvailable)
      us_dno_.resize(N);
    if (tollfreeAvailable)
      us_tollfree_.resize(N);
    if (lergAvailable)
      us_lerg_.resize(N);
    if (youmailAvailable)
      us_youmail_.resize(N);
    if (geoAvailable)
      us_geo_.resize(N);
    if (ftcAvailable)
      us_ftc_.resize(N);
    if (f404Available)
      us_f404_.resize(N);
    if (f606Available)
      us_f606_.resize(N);

    PhoneMapping::getUS()
      .getRNs(N, pn_.data(), us_rn_.data());
    PhoneMapping::getCA()
      .getRNs(N, pn_.data(), ca_rn_.data());

    if (dncAvailable)
      DncMapping::getDNC()
        .getDNCs(N, pn_.data(), us_dnc_.data());

    if (dnoAvailable)
      DnoMapping::getDNO()
        .getDNOs(N, pn_.data(), us_dno_.data());

    if (tollfreeAvailable)
      TollFreeMapping::getTollFree()
        .getTollFrees(N, pn_.data(), us_tollfree_.data());

    if (lergAvailable) {
      folly::small_vector<uint64_t, 16> lerg_search_key;
      lerg_search_key.resize(N);

      for (size_t i = 0; i < N; ++i) {
        uint64_t rn = us_rn_[i];
        if (rn == PhoneNumber::NONE)
          rn = ca_rn_[i];

        if (rn != PhoneNumber::NONE)
          lerg_search_key[i] = rn;
        else
          lerg_search_key[i] = pn_[i];
      }

      LergMapping::getLerg()
        .getLergs(N, lerg_search_key.data(), us_lerg_.data());
    }

    if (youmailAvailable)
      YoumailMapping::getYoumail()
        .getYoumails(N, pn_.data(), us_youmail_.data());

    if (geoAvailable)
      GeoMapping::getGeo()
        .getGeos(N, pn_.data(), us_geo_.data());

    if (ftcAvailable)
      FtcMapping::getFtc()
        .getFtcs(N, pn_.data(), us_ftc_.data());

    if (f404Available)
      F404Mapping::getF404()
        .getF404s(N, pn_.data(), us_f404_.data());

    if (f606Available)
      F606Mapping::getF606()
        .getF606s(N, pn_.data(), us_f606_.data());

    ResponseBuilder(downstream_)
      .status(200, "OK")
      .header(HTTP_HEADER_CONTENT_TYPE,
              json_ ? "application/json" : "text/plain")
      .send();

    if (json_)
      record += "[\n";
    for (size_t i = 0; i < N; ++i) {
      uint64_t rn = us_rn_[i];
      if (rn == PhoneNumber::NONE)
        rn = ca_rn_[i];

      std::string lrn_str = std::string("");
      std::string dno_str = std::string("");
      std::string dnc_str = std::string("");
      std::string tollfree_str = std::string("");
      std::string lerg_str = std::string("");
      std::string youmail_str = std::string("");
      std::string geo_str = std::string("");
      std::string ftc_str = std::string("");
      std::string f404_str = std::string("");
      std::string f606_str = std::string("");
      
      if (json_) {
        if (rn != PhoneNumber::NONE)
          lrn_str = folly::format("\"pn\": \"{}\", \"rn\": \"{}\"", pn_[i], rn).str();
        else
          lrn_str = folly::format("\"pn\": \"{}\", \"rn\": null", pn_[i]).str();

        if (!dncAvailable || us_dnc_[i] == 0)
          dnc_str = std::string("\"is_dnc\": \"no\"");
        else
          dnc_str = std::string("\"is_dnc\": \"yes\"");
          
        if (!dnoAvailable || us_dno_[i] == 0)
          dno_str = std::string("\"is_dno\": \"no\"");
        else
          dno_str = std::string("\"is_dno\": \"yes\"");

        if (!tollfreeAvailable || us_tollfree_[i] == 0)
          tollfree_str = std::string("\"is_tollfree\": \"no\"");
        else
          tollfree_str = std::string("\"is_tollfree\": \"yes\"");

        if (!lergAvailable || us_lerg_[i].lerg_key == 0)
          lerg_str = std::string("\"ocn\":: null, \"operator\": null, \"ocn_type\": null, \"lata\": null, \"rate_center\": null, \"country\": null");
        else {
          lerg_str = folly::format("\"ocn\": \"{}\", \"operator\": \"{}\", \"ocn_type\": \"{}\", \"lata\": \"{}\", \"rate_center\": \"{}\", \"country\": \"{}\"", 
            us_lerg_[i].ocn, us_lerg_[i].company, us_lerg_[i].ocn_type, us_lerg_[i].lata, us_lerg_[i].rate_center, us_lerg_[i].country).str();
        }

        if (!youmailAvailable || us_youmail_[i].pn == 0)
          youmail_str = std::string("\"youmail_SpamScore\": null, \"youmail_FraudProbability\": null, \"youmail_Unlawful\": null, \" youmail_TCPAFraudProbability\": null");
        else {
          youmail_str = folly::format("\"youmail_SpamScore\": \"{}\", \"youmail_FraudProbability\": \"{}\", \"youmail_Unlawful\": \"{}\", \"youmail_TCPAFraudProbability\": \"{}\"", 
            us_youmail_[i].sapmscore, us_youmail_[i].fraudprobability, us_youmail_[i].unlawful, us_youmail_[i].tcpafraud).str();
        }
        
        if (!geoAvailable || us_geo_[i].npanxx == 0)
          geo_str = std::string("\"zipcode\": null, \"county\": null, \"city\": null, \" latitude\": null, \" longitude\": null, \" timezone\": null");
        else {
          geo_str = folly::format("\"zipcode\": \"{}\", \"county\": \"{}\", \"city\": \"{}\", \"latitude\": \"{}\", \"longitude\": \"{}\", \"timezone\": \"{}\"", 
            us_geo_[i].zipcode, us_geo_[i].county, us_geo_[i].city, us_geo_[i].latitude, us_geo_[i].longitude, us_geo_[i].timezone).str();
        }

        if (!ftcAvailable || us_ftc_[i].pn == 0)
          ftc_str = std::string("\"is_ftc\": \"no\", \"last_ftc_on\": null, \"first_ftc_on\": null, \"ftc_count\": null");
        else {
          ftc_str = folly::format("\"is_ftc\": \"yes\", \"last_ftc_on\": \"{}\", \"first_ftc_on\": \"{}\", \" ftc_count\": \"{}\"", 
            us_ftc_[i].last_ftc_on, us_ftc_[i].first_ftc_on, us_ftc_[i].ftc_count).str();
        }

        if (!f404Available || us_f404_[i].pn == 0)
          f404_str = std::string("\"first_404_on\": null, \"last_404_on\": null");
        else {
          f404_str = folly::format("\"first_404_on\": \"{}\", \"last_404_on\": \"{}\"", 
            us_f404_[i].first_F404_on, us_f404_[i].last_F404_on).str();
        }

        if (!f606Available || us_f606_[i].pn == 0)
          f606_str = std::string("\"first_6xx_on\": null, \"last_6xx_on\": null");
        else {
          f606_str = folly::format("\"first_6xx_on\": \"{}\", \"last_6xx_on\": \"{}\"", 
            us_f606_[i].first_F606_on, us_f606_[i].last_F606_on).str();
        }

      } else {
        if (rn != PhoneNumber::NONE)
          lrn_str = folly::format("pn={},lrn={}", pn_[i], rn).str();
        else
          lrn_str = folly::format("pn={},lrn=null", pn_[i]).str();

        if (!dncAvailable || us_dnc_[i] == 0)
          dnc_str = std::string("is_dnc=no");
        else
          dnc_str = std::string("is_dnc=yes");
        
        if (!dnoAvailable || us_dno_[i] == 0)
          dno_str = std::string("is_dno=no");
        else
          dno_str = std::string("is_dno=yes");

        if (!tollfreeAvailable || us_tollfree_[i] == 0)
          tollfree_str = std::string("is_tollfree=no");
        else
          tollfree_str = std::string("is_tollfree=yes");
        
        if (!lergAvailable || us_lerg_[i].lerg_key == 0)
          lerg_str = std::string("ocn=null, operator=null, ocn_type=null, lata=null, rate_center=null, country=null ");
        else {
          lerg_str = folly::format("ocn={}, operator={}, ocn_type={}, lata={}, rate_center={}, country={}", 
            us_lerg_[i].ocn, us_lerg_[i].company, us_lerg_[i].ocn_type, us_lerg_[i].lata, us_lerg_[i].rate_center, us_lerg_[i].country).str();
        }

        if (!youmailAvailable || us_youmail_[i].pn == 0)
          youmail_str = std::string("youmail_SpamScore=null, youmail_FraudProbability=null, youmail_Unlawful=null, youmail_TCPAFraudProbability=null");
        else {
          youmail_str = folly::format("youmail_SpamScore={}, youmail_FraudProbability={}, youmail_Unlawful={}, youmail_TCPAFraudProbability={}", 
            us_youmail_[i].sapmscore, us_youmail_[i].fraudprobability, us_youmail_[i].unlawful, us_youmail_[i].tcpafraud).str();
        }

        if (!geoAvailable || us_geo_[i].npanxx == 0)
          geo_str = std::string("zipcode=null, county=null, city=null, latitude=null, longitude=null, timezone=null");
        else {
          geo_str = folly::format("zipcode={}, county={}, city={}, latitude={}, longitude={}, timezone={}", 
            us_geo_[i].zipcode, us_geo_[i].county, us_geo_[i].city, us_geo_[i].latitude, us_geo_[i].longitude, us_geo_[i].timezone).str();
        }

        if (!ftcAvailable || us_ftc_[i].pn == 0)
          ftc_str = std::string("is_ftc=no, last_ftc_on=null, first_ftc_on=null, ftc_count=null");
        else {
          ftc_str = folly::format("is_ftc=yes, last_ftc_on={}, first_ftc_on={}, ftc_count={}", 
            us_ftc_[i].last_ftc_on, us_ftc_[i].first_ftc_on, us_ftc_[i].ftc_count).str();
        }

        if (!f404Available || us_f404_[i].pn == 0)
          f404_str = std::string("first_404_on=null, last_404_on=null");
        else {
          f404_str = folly::format("first_404_on={}, last_404_on={}", 
            us_f404_[i].first_F404_on, us_f404_[i].last_F404_on).str();
        }

        if (!f606Available || us_f606_[i].pn == 0)
          f606_str = std::string("first_6xx_on=null, last_6xx_on=null");
        else {
          f606_str = folly::format("first_6xx_on={}, last_6xx_on={}", 
            us_f606_[i].first_F606_on, us_f606_[i].last_F606_on).str();
        }

      }

      if (i == N-1)
        folly::format(&record, "  {{{}, {}, {}, {}, {}, {}, {}, {}, {}, {}}}\n", lrn_str, dno_str, dnc_str, tollfree_str, lerg_str, youmail_str, geo_str, ftc_str, f404_str, f606_str);
      else
        folly::format(&record, "  {{{}, {}, {}, {}, {}, {}, {}, {}, {}, {}}},\n", lrn_str, dno_str, dnc_str, tollfree_str, lerg_str, youmail_str, geo_str, ftc_str, f404_str, f606_str);

      //if (record.size() > 1000) {   
      //  downstream_->sendBody(folly::IOBuf::copyBuffer(record));
      //  record.clear();
      //}
    }

    if (json_)
      record += "]\n";

    if (!record.empty())
      downstream_->sendBody(folly::IOBuf::copyBuffer(record));
    downstream_->sendEOM();
  }

  void onQueryString(StringPiece query) {
    using namespace std::placeholders;
    auto paramFn = std::bind(&TargetHandler::onQueryParam, this, _1, _2);
    HTTPMessage::splitNameValuePieces(query, '&', '=', std::move(paramFn));
  }

  void onQueryParam(StringPiece name, StringPiece value) {
    if (name == "phone%5B%5D" || name == "phone[]") {
      uint64_t pn = PhoneNumber::fromString(value);
      if (pn != PhoneNumber::NONE)
        pn_.push_back(pn);
    }
  }

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override {
    if (!needBody_)
      return;

    if (body_) {
      body_->prependChain(std::move(body));
    } else {
      body_ = std::move(body);
    }

    if (body_->computeChainDataLength() > FLAGS_max_query_length) {
      needBody_ = false;
      body_.release();
      ResponseBuilder(downstream_)
        .status(400, "Bad Request")
        .sendWithEOM();
    }
  }

  void onEOM() noexcept override {
    if (needBody_) {
      onQueryString(body_ ? StringPiece(body_->coalesce()) : "");
      onQueryComplete();
    }
  }

  void onUpgrade(UpgradeProtocol proto) noexcept override {
    // handler doesn't support upgrades
  }

  void requestComplete() noexcept override {
    delete this;
  }

  void onError(ProxygenError err) noexcept override {
    delete this;
  }

 private:
  bool needBody_ = true;
  bool json_ = false;
  std::unique_ptr<folly::IOBuf> body_;
  folly::small_vector<uint64_t, 16> pn_;
  folly::small_vector<uint64_t, 16> us_rn_;
  folly::small_vector<uint64_t, 16> ca_rn_;
  folly::small_vector<uint64_t, 16> us_dnc_;
  folly::small_vector<uint64_t, 16> us_dno_;
  folly::small_vector<uint64_t, 16> us_tollfree_;
  folly::small_vector<LergData, 16> us_lerg_;
  folly::small_vector<YoumailData, 16> us_youmail_;
  folly::small_vector<GeoData, 16> us_geo_;
  folly::small_vector<FtcData, 16> us_ftc_;
  folly::small_vector<F404Data, 16> us_f404_;
  folly::small_vector<F606Data, 16> us_f606_;
};

class ReverseHandler final : public RequestHandler {
 public:
  void onRequest(std::unique_ptr<HTTPMessage> req) noexcept override {
    using namespace std::placeholders;

    if (req->getMethod() != HTTPMethod::GET) {
      ResponseBuilder(downstream_)
        .status(400, "Bad Request")
        .sendWithEOM();
      return;
    }

    HTTPMessage::splitNameValuePieces(req->getQueryStringAsStringPiece(), '&', '=',
                                      std::bind(&ReverseHandler::onQueryParam,
                                                this, _1, _2));

    const std::string &accept = req->getHeaders()
      .getSingleOrEmpty(HTTP_HEADER_ACCEPT);
    bool json = isJsonRequested(accept);

    ResponseBuilder(downstream_)
      .status(200, "OK")
      .header(HTTP_HEADER_CONTENT_TYPE,
              json ? "application/json" : "text/plain")
      .send();

    PhoneMapping us = PhoneMapping::getUS();
    PhoneMapping ca = PhoneMapping::getCA();

    if (json)
      record_ += "[\n";
    for (std::pair<uint64_t, uint64_t> range : query_) {
      us.inverseRNs(range.first, range.second);
      sendBody(us, json);
      ca.inverseRNs(range.first, range.second);
      sendBody(ca, json);
    }
    if (json)
      record_ += "]\n";

    if (!record_.empty())
      downstream_->sendBody(folly::IOBuf::copyBuffer(record_));
    downstream_->sendEOM();
  }

  void sendBody(PhoneMapping &db, bool json) {
    for (; db.hasRow(); db.advance()) {
      if (json) {
        folly::format(&record_, "  {{\"pn\": \"{}\", \"rn\": \"{}\"}},\n",
                      db.currentPN(), db.currentRN());
      } else {
        folly::format(&record_, "{},{}\n", db.currentPN(), db.currentRN());
      }

      if (record_.size() > 1000) {
        downstream_->sendBody(folly::IOBuf::copyBuffer(record_));
        record_.clear();
      }
    }
  }

  void onQueryParam(StringPiece name, StringPiece value) {
    if (name == "prefix%5B%5D" || name == "prefix[]") {
      uint64_t from, to;

      if (value.size() > 10)
        return;

      if (auto asInt = folly::tryTo<uint64_t>(value))
        from = asInt.value();
      else
        return;

      to = from + 1;
      for (size_t i = 0; i < 10 - value.size(); ++i) {
        from *= 10;
        to *= 10;
      }
      query_.emplace_back(from, to);
    }
  }

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override {
  }

  void onEOM() noexcept override {
  }

  void onUpgrade(UpgradeProtocol proto) noexcept override {
    // handler doesn't support upgrades
  }

  void requestComplete() noexcept override {
    delete this;
  }

  void onError(ProxygenError err) noexcept override {
    delete this;
  }

 private:
  std::vector<std::pair<uint64_t, uint64_t>> query_;
  std::string record_;
};

class ApiHandlerFactory : public RequestHandlerFactory {
 public:
  void onServerStart(folly::EventBase* /*evb*/) noexcept override {
  }

  void onServerStop() noexcept override {
  }

  template<class H, class... Args>
  RequestHandler* makeHandler(Args&&... args)
  {
    if (LIKELY(PhoneMapping::isAvailable())) {
      return new H(std::forward(args)...);
    } else {
      return new DirectResponseHandler(503, "Service Unavailable", "");
    }
  }

  RequestHandler* onRequest(RequestHandler *upstream, HTTPMessage *msg) noexcept override {
    const StringPiece path = msg->getPathAsStringPiece();

    if (path == "/target") {
      return this->makeHandler<TargetHandler>();
    } else if (path == "/reverse") {
      return this->makeHandler<ReverseHandler>();
    } else {
      return new DirectResponseHandler(404, "Not found", "");
    }
  }
};

std::unique_ptr<RequestHandlerFactory> makeApiHandlerFactory()
{
  return std::make_unique<ApiHandlerFactory>();
}
