#include <curl/curl.h>
#include <time.h>
#include <unistd.h>
#include <set>
#include <vector>
#include <fstream>
#include <filesystem>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

#include "../../trunk-recorder/call_concluder/call_concluder.h"
#include "../../trunk-recorder/plugin_manager/plugin_api.h"
#include "../trunk-recorder/gr_blocks/decoder_wrapper.h"
#include <boost/dll/alias.hpp>
#include <boost/foreach.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <sys/stat.h>

struct Norasector_System {
  std::string short_name;
  int system_id;
};

struct Norasector_Uploader_Data {
  std::vector<Norasector_System> systems;
  std::string server;
  std::string api_key;
  std::string local_path;
  std::string transcribe_url;
};

static boost::mutex curl_share_mutex;

class Norasector_Uploader : public Plugin_Api {
  Norasector_Uploader_Data data;
  CURLSH *curl_share;
  long curl_dns_ttl;
  std::string plugin_name;

  struct queued_call_t {
    Call_Data_t call_info;
    std::string correlation_id;
  };

  std::vector<std::thread> worker_threads;
  int num_workers;
  std::queue<queued_call_t> work_queue;
  std::mutex queue_mutex;
  std::condition_variable queue_cv;
  std::atomic<bool> worker_running{false};

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

  // Create a staging copy of a file so the background worker owns it
  // independently of trunk-recorder's file cleanup. Tries hardlink first
  // (instant, no data copy), falls back to a full copy.
  static std::string stage_file(const char *src) {
    if (!src || src[0] == '\0') return "";
    std::string staged = std::string(src) + ".staging";
    if (::link(src, staged.c_str()) == 0) return staged;
    std::error_code ec;
    std::filesystem::copy_file(src, staged, std::filesystem::copy_options::overwrite_existing, ec);
    if (!ec) return staged;
    BOOST_LOG_TRIVIAL(error) << "\t[Norasector]\t" << "Failed to stage " << src << ": " << ec.message();
    return "";
  }

  static size_t write_callback(void *contents, size_t size, size_t nmemb, void *userp) {
    ((std::string *)userp)->append((char *)contents, size * nmemb);
    return size * nmemb;
  }

  int upload(Call_Data_t call_info, const std::string &correlation_id) {
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
    json_ss << ",\"correlation_id\":\"" << correlation_id << "\"";
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
      curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60L);
      curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
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

  std::string save_local(Call_Data_t call_info) {
    Norasector_System *sys = get_system(call_info.short_name);
    if (!sys) {
      return "";
    }

    std::string loghdr = log_header(call_info.short_name, call_info.call_num, call_info.talkgroup_display, call_info.freq);

    struct tm tm_buf;
    time_t start = call_info.start_time;
    localtime_r(&start, &tm_buf);

    // Build path: {localPath}/{shortName}/{YYYY}/{M}/{D}/{tgid}-{start_time}_{freq}.wav
    std::ostringstream dir_ss;
    dir_ss << data.local_path << "/" << call_info.short_name
           << "/" << (tm_buf.tm_year + 1900)
           << "/" << (tm_buf.tm_mon + 1)
           << "/" << tm_buf.tm_mday;
    std::string dir_path = dir_ss.str();

    std::ostringstream file_ss;
    file_ss << dir_path << "/" << call_info.talkgroup
            << "-" << call_info.start_time
            << "_" << (long)call_info.freq << ".wav";
    std::string out_path = file_ss.str();

    std::error_code ec;
    std::filesystem::create_directories(dir_path, ec);
    if (ec) {
      BOOST_LOG_TRIVIAL(error) << loghdr << this->plugin_name << " Failed to create directory " << dir_path << ": " << ec.message();
      return "";
    }

    std::ifstream src(call_info.filename, std::ios::binary);
    if (!src) {
      BOOST_LOG_TRIVIAL(error) << loghdr << this->plugin_name << " Failed to open source file: " << call_info.filename;
      return "";
    }

    std::ofstream dst(out_path, std::ios::binary);
    if (!dst) {
      BOOST_LOG_TRIVIAL(error) << loghdr << this->plugin_name << " Failed to open destination file: " << out_path;
      return "";
    }

    dst << src.rdbuf();

    if (!dst.good()) {
      BOOST_LOG_TRIVIAL(error) << loghdr << this->plugin_name << " Failed to write local file: " << out_path;
      return "";
    }

    BOOST_LOG_TRIVIAL(info) << loghdr << this->plugin_name << " Saved local copy: " << out_path;
    return out_path;
  }

  void notify_transcribe(Call_Data_t call_info, const std::string &local_file, const std::string &correlation_id) {
    Norasector_System *sys = get_system(call_info.short_name);
    if (!sys) {
      return;
    }

    std::string loghdr = log_header(call_info.short_name, call_info.call_num, call_info.talkgroup_display, call_info.freq);

    // Deduplicate source IDs
    std::set<long> source_set;
    for (unsigned long i = 0; i < call_info.transmission_source_list.size(); i++) {
      source_set.insert(call_info.transmission_source_list[i].source);
    }

    std::ostringstream src_ids_ss;
    src_ids_ss << "[";
    bool first = true;
    for (std::set<long>::iterator it = source_set.begin(); it != source_set.end(); ++it) {
      if (!first) {
        src_ids_ss << ",";
      }
      src_ids_ss << *it;
      first = false;
    }
    src_ids_ss << "]";

    std::ostringstream json_ss;
    json_ss << "{";
    json_ss << "\"file_path\":\"" << local_file << "\"";
    json_ss << ",\"tgid\":" << call_info.talkgroup;
    json_ss << ",\"system_id\":" << sys->system_id;
    json_ss << ",\"start_time\":" << call_info.start_time;
    json_ss << ",\"src_ids\":" << src_ids_ss.str();
    json_ss << ",\"correlation_id\":\"" << correlation_id << "\"";
    json_ss << "}";
    std::string json_str = json_ss.str();

    CURL *curl = curl_easy_init();
    if (!curl) {
      BOOST_LOG_TRIVIAL(error) << loghdr << this->plugin_name << " Failed to init curl for transcribe notify";
      return;
    }

    std::string response_buffer;
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(curl, CURLOPT_URL, data.transcribe_url.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_str.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_buffer);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "TrunkRecorder1.0");

    CURLcode res = curl_easy_perform(curl);

    if (res == CURLE_OK) {
      long response_code;
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
      BOOST_LOG_TRIVIAL(info) << loghdr << this->plugin_name << " Transcribe notify sent, response: " << response_code;
    } else {
      BOOST_LOG_TRIVIAL(error) << loghdr << this->plugin_name << " Transcribe notify failed: " << curl_easy_strerror(res);
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }

  void worker_loop(int worker_id) {
    std::string wid = "[worker " + std::to_string(worker_id) + "] ";
    while (true) {
      queued_call_t item;
      size_t remaining;
      {
        std::unique_lock<std::mutex> lock(queue_mutex);
        queue_cv.wait(lock, [this] { return !work_queue.empty() || !worker_running.load(); });
        if (work_queue.empty()) {
          break;
        }
        item = work_queue.front();
        work_queue.pop();
        remaining = work_queue.size();
      }

      if (remaining > 0) {
        std::string loghdr = log_header(item.call_info.short_name, item.call_info.call_num, item.call_info.talkgroup_display, item.call_info.freq);
        BOOST_LOG_TRIVIAL(info) << loghdr << this->plugin_name << " " << wid << "processing, " << remaining << " still queued";
      }

      upload(item.call_info, item.correlation_id);

      std::error_code ec;
      std::filesystem::remove(item.call_info.converted, ec);

      if (!data.local_path.empty()) {
        std::string local_file = save_local(item.call_info);
        std::filesystem::remove(item.call_info.filename, ec);
        if (!local_file.empty() && !data.transcribe_url.empty()) {
          notify_transcribe(item.call_info, local_file, item.correlation_id);
        }
      }
    }
  }

  int start() override {
    worker_running.store(true);
    worker_threads.reserve(num_workers);
    for (int i = 0; i < num_workers; i++) {
      worker_threads.emplace_back(&Norasector_Uploader::worker_loop, this, i);
    }
    BOOST_LOG_TRIVIAL(info) << "\t[Norasector]\tStarted " << num_workers << " upload worker threads";
    return 0;
  }

  int stop() override {
    {
      std::lock_guard<std::mutex> lock(queue_mutex);
      worker_running.store(false);
    }
    queue_cv.notify_all();
    for (auto &t : worker_threads) {
      if (t.joinable()) {
        t.join();
      }
    }
    worker_threads.clear();
    return 0;
  }

  int call_end(Call_Data_t call_info) override {
    // Stage files before trunk-recorder deletes them after this returns.
    // Hardlink is instant; copy is the fallback for cross-filesystem cases.
    if (call_info.converted[0] != '\0') {
      std::string s = stage_file(call_info.converted);
      if (!s.empty())
        snprintf(call_info.converted, sizeof(call_info.converted), "%s", s.c_str());
    }
    if (!data.local_path.empty() && call_info.filename[0] != '\0') {
      std::string s = stage_file(call_info.filename);
      if (!s.empty())
        snprintf(call_info.filename, sizeof(call_info.filename), "%s", s.c_str());
    }
    std::string correlation_id = boost::uuids::to_string(boost::uuids::random_generator()());
    {
      std::lock_guard<std::mutex> lock(queue_mutex);
      work_queue.push({call_info, correlation_id});
    }
    queue_cv.notify_one();
    return 0;
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
    while (!this->data.server.empty() && this->data.server.back() == '/') {
      this->data.server.pop_back();
    }
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

    this->num_workers = config_data.value("uploadThreads", 4);
    if (this->num_workers < 1) this->num_workers = 1;
    if (this->num_workers > 16) this->num_workers = 16;
    BOOST_LOG_TRIVIAL(info) << log_prefix << "Upload threads: " << this->num_workers;

    this->data.local_path = config_data.value("localPath", "");
    while (!this->data.local_path.empty() && this->data.local_path.back() == '/') {
      this->data.local_path.pop_back();
    }
    this->data.transcribe_url = config_data.value("transcribeUrl", "");

    if (!this->data.local_path.empty()) {
      BOOST_LOG_TRIVIAL(info) << log_prefix << "Local path: " << this->data.local_path;
    }
    if (!this->data.transcribe_url.empty()) {
      BOOST_LOG_TRIVIAL(info) << log_prefix << "Transcribe URL: " << this->data.transcribe_url;
      if (this->data.local_path.empty()) {
        BOOST_LOG_TRIVIAL(warning) << log_prefix << "transcribeUrl is set but localPath is not — transcription notifications will be skipped";
      }
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
