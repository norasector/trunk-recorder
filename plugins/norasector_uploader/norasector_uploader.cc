#include <curl/curl.h>
#include <time.h>
#include <set>
#include <vector>

#include "../../trunk-recorder/call_concluder/call_concluder.h"
#include "../../trunk-recorder/plugin_manager/plugin_api.h"
#include "../trunk-recorder/gr_blocks/decoder_wrapper.h"
#include <boost/dll/alias.hpp>
#include <boost/foreach.hpp>
#include <sys/stat.h>

struct Norasector_System {
  std::string short_name;
  int system_id;
};

struct Norasector_Uploader_Data {
  std::vector<Norasector_System> systems;
  std::string server;
  std::string api_key;
};

boost::mutex curl_share_mutex;

class Norasector_Uploader : public Plugin_Api {
  Norasector_Uploader_Data data;
  CURLSH *curl_share;
  long curl_dns_ttl;
  std::string plugin_name;

public:
  Norasector_System *get_system(std::string short_name) {
    for (std::vector<Norasector_System>::iterator it = data.systems.begin(); it != data.systems.end(); ++it) {
      Norasector_System sys = *it;
      if (sys.short_name == short_name) {
        return &(*it);
      }
    }
    return NULL;
  }

  static size_t write_callback(void *contents, size_t size, size_t nmemb, void *userp) {
    ((std::string *)userp)->append((char *)contents, size * nmemb);
    return size * nmemb;
  }

  int upload(Call_Data_t call_info) {
    Norasector_System *sys = get_system(call_info.short_name);

    if (!sys) {
      return 0;
    }

    // Build RFC3339 timestamp from unix epoch
    char timestamp_buf[64];
    struct tm tm_buf;
    time_t start = call_info.start_time;
    gmtime_r(&start, &tm_buf);
    strftime(timestamp_buf, sizeof(timestamp_buf), "%Y-%m-%dT%H:%M:%SZ", &tm_buf);

    // Deduplicate source IDs
    std::set<long> source_set;
    for (unsigned long i = 0; i < call_info.transmission_source_list.size(); i++) {
      source_set.insert(call_info.transmission_source_list[i].source);
    }

    // Build source_ids JSON array
    std::ostringstream source_ids_ss;
    source_ids_ss << "[";
    bool first = true;
    for (std::set<long>::iterator it = source_set.begin(); it != source_set.end(); ++it) {
      if (!first) {
        source_ids_ss << ",";
      }
      source_ids_ss << *it;
      first = false;
    }
    source_ids_ss << "]";

    int length_ms = (int)(call_info.length * 1000);

    // Build JSON metadata
    std::ostringstream json_ss;
    json_ss << "{";
    json_ss << "\"system_id\":" << sys->system_id;
    json_ss << ",\"tgid\":" << call_info.talkgroup;
    json_ss << ",\"timestamp\":\"" << timestamp_buf << "\"";
    json_ss << ",\"length_ms\":" << length_ms;
    json_ss << ",\"source_ids\":" << source_ids_ss.str();
    json_ss << "}";
    std::string json_str = json_ss.str();

    CURLMcode res;
    CURLM *multi_handle;
    int still_running = 0;
    std::string response_buffer;

    struct curl_slist *headerlist = NULL;

    CURL *curl = curl_easy_init();
    curl_mime *mime;
    curl_mimepart *part;

    mime = curl_mime_init(curl);

    // Part 1: JSON metadata
    part = curl_mime_addpart(mime);
    curl_mime_data(part, json_str.c_str(), CURL_ZERO_TERMINATED);
    curl_mime_type(part, "application/json");

    // Part 2: M4A audio file
    part = curl_mime_addpart(mime);
    curl_mime_filedata(part, call_info.converted);
    curl_mime_type(part, "audio/m4a");

    multi_handle = curl_multi_init();

    headerlist = curl_slist_append(headerlist, "Expect:");
    std::string api_key_header = "x-api-key: " + data.api_key;
    headerlist = curl_slist_append(headerlist, api_key_header.c_str());

    if (curl && multi_handle) {
      std::string url = data.server + "/recordings";

      curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
      curl_easy_setopt(curl, CURLOPT_USERAGENT, "TrunkRecorder1.0");
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerlist);
      curl_easy_setopt(curl, CURLOPT_MIMEPOST, mime);
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_buffer);
      curl_easy_setopt(curl, CURLOPT_SHARE, curl_share);
      curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT, curl_dns_ttl);

      curl_multi_add_handle(multi_handle, curl);
      curl_multi_perform(multi_handle, &still_running);

      while (still_running) {
        struct timeval timeout;
        int rc;
        CURLMcode mc;

        fd_set fdread;
        fd_set fdwrite;
        fd_set fdexcep;
        int maxfd = -1;

        long curl_timeo = -1;

        FD_ZERO(&fdread);
        FD_ZERO(&fdwrite);
        FD_ZERO(&fdexcep);

        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        curl_multi_timeout(multi_handle, &curl_timeo);
        if (curl_timeo >= 0) {
          timeout.tv_sec = curl_timeo / 1000;
          if (timeout.tv_sec > 1)
            timeout.tv_sec = 1;
          else
            timeout.tv_usec = (curl_timeo % 1000) * 1000;
        }

        mc = curl_multi_fdset(multi_handle, &fdread, &fdwrite, &fdexcep, &maxfd);

        if (mc != CURLM_OK) {
          fprintf(stderr, "curl_multi_fdset() failed, code %d.\n", mc);
          break;
        }

        if (maxfd == -1) {
          struct timeval wait = {0, 100 * 1000}; /* 100ms */
          rc = select(0, NULL, NULL, NULL, &wait);
        } else {
          rc = select(maxfd + 1, &fdread, &fdwrite, &fdexcep, &timeout);
        }

        switch (rc) {
        case -1:
          break;
        case 0:
        default:
          curl_multi_perform(multi_handle, &still_running);
          break;
        }
      }

      long response_code;
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

      res = curl_multi_cleanup(multi_handle);
      curl_easy_cleanup(curl);
      curl_mime_free(mime);
      curl_slist_free_all(headerlist);

      if (res == CURLM_OK && response_code == 200) {
        struct stat file_info;
        stat(call_info.converted, &file_info);
        std::string loghdr = log_header(call_info.short_name, call_info.call_num, call_info.talkgroup_display, call_info.freq);
        BOOST_LOG_TRIVIAL(info) << loghdr << this->plugin_name << " Upload Success - file size: " << file_info.st_size;
        return 0;
      }
    }

    std::string loghdr = log_header(call_info.short_name, call_info.call_num, call_info.talkgroup_display, call_info.freq);
    BOOST_LOG_TRIVIAL(error) << loghdr << this->plugin_name << " Upload Error: " << response_buffer;
    return 1;
  }

  int call_end(Call_Data_t call_info) {
    return upload(call_info);
  }

  int parse_config(json config_data) {
    this->plugin_name = "Norasector";
    std::string log_prefix = "\t[Norasector]\t";

    bool server_exists = config_data.contains("server");
    if (!server_exists) {
      BOOST_LOG_TRIVIAL(error) << log_prefix << "server not specified in config";
      return 1;
    }

    this->data.server = config_data.value("server", "");
    BOOST_LOG_TRIVIAL(info) << log_prefix << "Server: " << this->data.server;

    boost::regex ex("(http|https)://([^/ :]+):?([^/ ]*)(/?[^ #?]*)\\x3f?([^ #]*)#?([^ ]*)");
    boost::cmatch what;

    if (!regex_match(this->data.server.c_str(), what, ex)) {
      BOOST_LOG_TRIVIAL(error) << log_prefix << "Unable to parse Server URL";
      return 1;
    }

    if (!config_data.contains("apiKey")) {
      BOOST_LOG_TRIVIAL(error) << log_prefix << "apiKey not specified in config";
      return 1;
    }
    this->data.api_key = config_data.value("apiKey", "");

    if (!config_data.contains("systems")) {
      BOOST_LOG_TRIVIAL(error) << log_prefix << "No systems configured";
      return 1;
    }

    for (json element : config_data["systems"]) {
      Norasector_System sys;
      sys.short_name = element.value("shortName", "");
      sys.system_id = element.value("systemId", 0);

      if (sys.short_name.empty() || sys.system_id == 0) {
        BOOST_LOG_TRIVIAL(error) << log_prefix << "System entry missing shortName or systemId";
        continue;
      }

      BOOST_LOG_TRIVIAL(info) << log_prefix << "Uploading calls for: " << sys.short_name << "\tSystem ID: " << sys.system_id;
      this->data.systems.push_back(sys);
    }

    if (this->data.systems.size() == 0) {
      BOOST_LOG_TRIVIAL(error) << log_prefix << "Server set, but no systems are configured";
      return 1;
    }

    curl_share = curl_share_init();
    curl_share_setopt(curl_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
    curl_share_setopt(curl_share, CURLSHOPT_LOCKFUNC, curl_lock_cb);
    curl_share_setopt(curl_share, CURLSHOPT_UNLOCKFUNC, curl_unlock_cb);
    curl_dns_ttl = 300;

    return 0;
  }

  static void curl_lock_cb(CURL *handle, curl_lock_data data, curl_lock_access access, void *userptr) {
    curl_share_mutex.lock();
  }

  static void curl_unlock_cb(CURL *handle, curl_lock_data data, curl_lock_access access, void *userptr) {
    curl_share_mutex.unlock();
  }

  static boost::shared_ptr<Norasector_Uploader> create() {
    return boost::shared_ptr<Norasector_Uploader>(
        new Norasector_Uploader());
  }
};

BOOST_DLL_ALIAS(
    Norasector_Uploader::create,
    create_plugin
)
