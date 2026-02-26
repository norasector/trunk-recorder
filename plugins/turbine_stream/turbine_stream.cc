#include "../../trunk-recorder/plugin_manager/plugin_api.h"
#include "../../trunk-recorder/recorders/recorder.h"
#include <boost/dll/alias.hpp>
#include <boost/foreach.hpp>
#include <boost/asio.hpp>
#include <boost/log/trivial.hpp>

#include <opus/opus.h>
#include "opus_frame.pb.h"
#include <google/protobuf/timestamp.pb.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstring>
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

using namespace boost::asio;

// Polyphase FIR upsampler for integer-ratio sample rate conversion.
// Uses a windowed-sinc lowpass filter decomposed into polyphase components.
struct Upsampler {
  int factor;
  int taps_per_phase;
  std::vector<std::vector<float>> coeffs; // [phase][tap]
  std::vector<float> history;

  Upsampler() : factor(1), taps_per_phase(0) {}

  void init(int upsample_factor, int num_taps_per_phase = 24) {
    factor = upsample_factor;
    if (factor <= 1) {
      factor = 1;
      return;
    }
    taps_per_phase = num_taps_per_phase;
    int total_taps = factor * taps_per_phase;

    // Prototype lowpass filter: windowed sinc at cutoff = pi/factor
    // Blackman window for ~58dB stopband attenuation (vs ~43dB for Hamming)
    std::vector<float> h(total_taps);
    float center = (total_taps - 1) / 2.0f;
    for (int n = 0; n < total_taps; n++) {
      float x = n - center;
      float sinc_val;
      if (fabsf(x) < 1e-6f) {
        sinc_val = 1.0f;
      } else {
        sinc_val = sinf((float)M_PI * x / factor) / ((float)M_PI * x / factor);
      }
      float t = 2.0f * (float)M_PI * n / (total_taps - 1);
      float window = 0.42f - 0.5f * cosf(t) + 0.08f * cosf(2.0f * t);
      h[n] = (float)factor * sinc_val * window;
    }

    // Decompose into polyphase components
    coeffs.resize(factor);
    for (int p = 0; p < factor; p++) {
      coeffs[p].resize(taps_per_phase);
      for (int t = 0; t < taps_per_phase; t++) {
        coeffs[p][t] = h[p + t * factor];
      }
    }

    history.assign(taps_per_phase, 0.0f);
  }

  void reset() {
    if (taps_per_phase > 0) {
      history.assign(taps_per_phase, 0.0f);
    }
  }

  // Convert int16 input at original rate to float output at upsampled rate
  void process(const int16_t *input, int count, std::vector<float> &output) {
    if (factor <= 1) {
      for (int i = 0; i < count; i++) {
        output.push_back(input[i] / 32768.0f);
      }
      return;
    }

    output.reserve(output.size() + count * factor);
    for (int i = 0; i < count; i++) {
      for (int t = 0; t < taps_per_phase - 1; t++) {
        history[t] = history[t + 1];
      }
      history[taps_per_phase - 1] = input[i] / 32768.0f;

      for (int p = 0; p < factor; p++) {
        float sum = 0.0f;
        for (int t = 0; t < taps_per_phase; t++) {
          sum += coeffs[p][t] * history[taps_per_phase - 1 - t];
        }
        output.push_back(sum);
      }
    }
  }
};

// Biquad filter (Direct Form II Transposed)
struct Biquad {
  float b0, b1, b2, a1, a2;
  float z1, z2; // state

  Biquad() : b0(1), b1(0), b2(0), a1(0), a2(0), z1(0), z2(0) {}

  void reset() { z1 = z2 = 0; }

  // Configure as high-shelf: boost frequencies above f0
  // gain_db: amount to boost (e.g. 6.0), f0: corner frequency, fs: sample rate
  void init_high_shelf(float gain_db, float f0, float fs) {
    float A = sqrtf(powf(10.0f, gain_db / 20.0f));
    float w0 = 2.0f * (float)M_PI * f0 / fs;
    float cosw0 = cosf(w0);
    float sinw0 = sinf(w0);
    float alpha = sinw0 / 2.0f * sqrtf((A + 1.0f / A) * 2.0f);

    float a0 =          (A + 1) - (A - 1) * cosw0 + 2 * sqrtf(A) * alpha;
    b0 =           (A * ((A + 1) + (A - 1) * cosw0 + 2 * sqrtf(A) * alpha)) / a0;
    b1 = (-2 * A * ((A - 1) + (A + 1) * cosw0))                             / a0;
    b2 =           (A * ((A + 1) + (A - 1) * cosw0 - 2 * sqrtf(A) * alpha)) / a0;
    a1 =      (2 * ((A - 1) - (A + 1) * cosw0))                             / a0;
    a2 =           ((A + 1) - (A - 1) * cosw0 - 2 * sqrtf(A) * alpha)       / a0;
    z1 = z2 = 0;
  }

  float process(float x) {
    float y = b0 * x + z1;
    z1 = b1 * x - a1 * y + z2;
    z2 = b2 * x - a2 * y;
    return y;
  }
};

struct turbine_stream_t {
  long TGID;
  std::string address;
  std::string short_name;
  long port;
  ip::udp::endpoint remote_endpoint;
  int opus_bitrate;
  int opus_frame_ms;
};

struct queued_packet_t {
  std::vector<char> data;
  std::string short_name;
  uint32_t tgid;
};

struct call_state_t {
  OpusEncoder *encoder;
  long encoder_sample_rate;
  int frame_size;
  std::vector<float> buffer;
  uint64_t frame_number;
  std::chrono::system_clock::time_point start_time;
  uint64_t total_samples_encoded;
  uint32_t system_id;
  uint32_t tgid;
  uint32_t src_id;
  std::string short_name;
  Upsampler upsampler;
  Biquad high_shelf;
  int send_interval_ms;
  std::queue<queued_packet_t> packet_queue;
  std::chrono::steady_clock::time_point next_send_time;
  long call_num;
  bool sending_started;
};

class Turbine_Stream : public Plugin_Api {
  typedef boost::asio::io_service io_service;
  io_service my_io_service;
  ip::udp::socket my_socket{my_io_service};

  std::vector<turbine_stream_t> streams;
  std::map<Recorder *, call_state_t> call_states;
  std::map<long, Recorder *> call_num_to_recorder;
  std::mutex call_states_mutex;

  int opus_bitrate = 0; // 0 = OPUS_AUTO (matches turbine)
  int opus_frame_ms = 40; // 40ms frames (matches turbine)
  int opus_sample_rate = 24000;
  float treble_boost_db = 0; // high-shelf boost in dB (0 = off)
  float treble_freq = 1000;  // high-shelf corner frequency
  int send_buffer_frames = 4; // require this many queued frames before sending (jitter buffer)

  std::thread sender_thread;
  std::atomic<bool> sender_running{false};

  // Encode complete frames from the sample buffer into the packet queue
  void encode_frames(call_state_t &state) {
    int frame_size = state.frame_size;
    std::vector<unsigned char> opus_out(4000);

    while ((int)state.buffer.size() >= frame_size) {
      int encoded_bytes = opus_encode_float(state.encoder, state.buffer.data(),
                                            frame_size, opus_out.data(), (int)opus_out.size());

      state.buffer.erase(state.buffer.begin(), state.buffer.begin() + frame_size);

      if (encoded_bytes < 0) {
        BOOST_LOG_TRIVIAL(error) << "[turbine_stream] opus_encode_float error: "
                                 << opus_strerror(encoded_bytes);
        continue;
      }

      turbine::TaggedOpusFrame frame;
      frame.set_system_id(state.system_id);
      frame.set_tgid(state.tgid);
      frame.set_src_id(state.src_id);

      uint32_t sample_duration_us = (uint32_t)((uint64_t)frame_size * 1000000 / state.encoder_sample_rate);
      frame.set_sample_length_microseconds(sample_duration_us);

      auto frame_offset = std::chrono::microseconds(
          (int64_t)state.total_samples_encoded * 1000000 / state.encoder_sample_rate);
      auto frame_time = state.start_time + frame_offset;
      auto epoch = frame_time.time_since_epoch();
      auto seconds = std::chrono::duration_cast<std::chrono::seconds>(epoch);
      auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(epoch) -
                   std::chrono::duration_cast<std::chrono::nanoseconds>(seconds);

      google::protobuf::Timestamp *ts = frame.mutable_ts();
      ts->set_seconds(seconds.count());
      ts->set_nanos((int32_t)nanos.count());

      frame.set_frame_number(state.frame_number++);
      state.total_samples_encoded += frame_size;
      frame.set_data(opus_out.data(), encoded_bytes);

      std::string serialized;
      frame.SerializeToString(&serialized);

      uint16_t len = (uint16_t)serialized.size();
      std::vector<char> packet(2 + serialized.size());
      packet[0] = (char)(len & 0xFF);
      packet[1] = (char)((len >> 8) & 0xFF);
      std::memcpy(packet.data() + 2, serialized.data(), serialized.size());

      state.packet_queue.push({std::move(packet), state.short_name, state.tgid});
    }
  }

  // Send a single packet to all matching streams
  void send_packet(const queued_packet_t &pkt) {
    boost::system::error_code error;
    BOOST_FOREACH (auto &stream, streams) {
      if (stream.short_name.empty() || stream.short_name == pkt.short_name) {
        if (stream.TGID == 0 || stream.TGID == (long)pkt.tgid) {
          my_socket.send_to(buffer(pkt.data.data(), pkt.data.size()),
                            stream.remote_endpoint, 0, error);
        }
      }
    }
  }

  // Background thread: drains per-call packet queues at the correct pace
  void sender_loop() {
    while (sender_running.load(std::memory_order_relaxed)) {
      auto now = std::chrono::steady_clock::now();
      auto next_wake = now + std::chrono::milliseconds(5);
      std::vector<queued_packet_t> to_send;

      {
        std::lock_guard<std::mutex> lock(call_states_mutex);
        for (auto &pair : call_states) {
          call_state_t &state = pair.second;
          if (!state.sending_started) {
            if ((int)state.packet_queue.size() < send_buffer_frames) continue;
            state.sending_started = true;
          }
          if (state.packet_queue.empty()) continue;
          if (now < state.next_send_time) {
            next_wake = std::min(next_wake, state.next_send_time);
            continue;
          }

          to_send.push_back(std::move(state.packet_queue.front()));
          state.packet_queue.pop();
          state.next_send_time = now + std::chrono::milliseconds(state.send_interval_ms);

          if (!state.packet_queue.empty()) {
            next_wake = std::min(next_wake, state.next_send_time);
          }
        }
      }

      for (auto &pkt : to_send) {
        send_packet(pkt);
      }

      std::this_thread::sleep_until(next_wake);
    }
  }

public:
  Turbine_Stream() {}

  int parse_config(json config_data) {
    opus_sample_rate = config_data.value("opusSampleRate", opus_sample_rate);
    opus_frame_ms = config_data.value("opusFrameMs", opus_frame_ms);
    opus_bitrate = config_data.value("opusBitrate", opus_bitrate);
    treble_boost_db = config_data.value("trebleBoostDb", treble_boost_db);
    treble_freq = config_data.value("trebleFreq", treble_freq);
    send_buffer_frames = config_data.value("sendBufferFrames", send_buffer_frames);
    if (treble_boost_db != 0) {
      BOOST_LOG_TRIVIAL(info) << "[turbine_stream] treble boost: " << treble_boost_db << "dB above " << treble_freq << "Hz";
    }

    for (json element : config_data["streams"]) {
      turbine_stream_t stream;
      stream.TGID = element.value("TGID", (long)0);
      stream.address = element["address"];
      stream.port = element["port"];
      {
        ip::udp::resolver resolver(my_io_service);
        ip::udp::resolver::query query(stream.address, std::to_string(stream.port));
        boost::system::error_code ec;
        auto results = resolver.resolve(query, ec);
        if (ec || results == ip::udp::resolver::iterator()) {
          BOOST_LOG_TRIVIAL(error) << "[turbine_stream] failed to resolve " << stream.address << ": " << ec.message();
          continue;
        }
        // Prefer v6, fall back to v4-mapped-to-v6 for dual-stack socket
        ip::udp::endpoint chosen = *results;
        for (auto it = results; it != ip::udp::resolver::iterator(); ++it) {
          ip::udp::endpoint ep = *it;
          if (ep.address().is_v6()) {
            chosen = ep;
            break;
          }
        }
        if (chosen.address().is_v4()) {
          chosen = ip::udp::endpoint(ip::address_v6::v4_mapped(chosen.address().to_v4()), chosen.port());
        }
        stream.remote_endpoint = chosen;
      }
      stream.short_name = element.value("shortName", std::string(""));
      stream.opus_bitrate = element.value("opusBitrate", opus_bitrate);
      stream.opus_frame_ms = element.value("opusFrameMs", opus_frame_ms);

      BOOST_LOG_TRIVIAL(info) << "[turbine_stream] streaming TGID " << stream.TGID
                              << " on system \"" << stream.short_name << "\""
                              << " to " << stream.address << ":" << stream.port
                              << " (opus bitrate=" << (stream.opus_bitrate <= 0 ? "auto" : std::to_string(stream.opus_bitrate))
                              << ", frame=" << stream.opus_frame_ms << "ms"
                              << ", sampleRate=" << opus_sample_rate << ")";
      streams.push_back(stream);
    }
    return 0;
  }

  int start() {
    my_socket.open(ip::udp::v6());
    my_socket.set_option(ip::v6_only(false));
    sender_running.store(true);
    sender_thread = std::thread(&Turbine_Stream::sender_loop, this);
    BOOST_LOG_TRIVIAL(info) << "[turbine_stream] started with " << streams.size() << " stream(s)";
    return 0;
  }

  int stop() {
    sender_running.store(false);
    if (sender_thread.joinable()) {
      sender_thread.join();
    }

    std::lock_guard<std::mutex> lock(call_states_mutex);
    for (auto &pair : call_states) {
      if (pair.second.encoder) {
        opus_encoder_destroy(pair.second.encoder);
        pair.second.encoder = nullptr;
      }
    }
    call_states.clear();
    call_num_to_recorder.clear();
    my_socket.close();
    BOOST_LOG_TRIVIAL(info) << "[turbine_stream] stopped";
    return 0;
  }

  int call_start(Call *call) {
    Recorder *rec = call->get_recorder();
    if (!rec) return 0;
    std::lock_guard<std::mutex> lock(call_states_mutex);
    call_num_to_recorder[call->get_call_num()] = rec;
    return 0;
  }

  int call_end(Call_Data_t call_info) {
    std::lock_guard<std::mutex> lock(call_states_mutex);

    auto num_it = call_num_to_recorder.find(call_info.call_num);
    if (num_it == call_num_to_recorder.end()) return 0;

    Recorder *recorder = num_it->second;
    call_num_to_recorder.erase(num_it);

    auto it = call_states.find(recorder);
    if (it == call_states.end()) return 0;

    call_state_t &state = it->second;

    // Only clean up state if it still belongs to this call.
    // If a new call has already started on this recorder (detected in
    // audio_stream), the state's call_num will have changed and we
    // must not destroy the new call's active encoder.
    if (state.call_num != call_info.call_num) return 0;

    // Flush remaining samples padded with silence
    if (!state.buffer.empty() && state.encoder) {
      state.buffer.resize(state.frame_size, 0.0f);
      encode_frames(state);
    }

    // Drain any queued packets immediately (call is ending)
    while (!state.packet_queue.empty()) {
      send_packet(state.packet_queue.front());
      state.packet_queue.pop();
    }

    if (state.encoder) {
      opus_encoder_destroy(state.encoder);
    }
    call_states.erase(it);
    return 0;
  }

  int audio_stream(Call *call, Recorder *recorder, int16_t *samples, int sampleCount) {
    System *call_system = call->get_system();
    int32_t call_tgid = call->get_talkgroup();
    std::string call_short_name = call->get_short_name();

    bool any_match = false;
    int match_bitrate = opus_bitrate;
    int match_frame_ms = opus_frame_ms;
    BOOST_FOREACH (auto &stream, streams) {
      if (stream.short_name.empty() || stream.short_name == call_short_name) {
        if (stream.TGID == 0 || stream.TGID == call_tgid) {
          any_match = true;
          match_bitrate = stream.opus_bitrate;
          match_frame_ms = stream.opus_frame_ms;
          break;
        }
      }
    }
    if (!any_match) return 0;

    long wav_hz = recorder->get_wav_hz();
    long current_call_num = call->get_call_num();

    std::lock_guard<std::mutex> lock(call_states_mutex);
    auto it = call_states.find(recorder);

    // Detect call transition: same recorder, different call.
    // This happens when the GNU Radio pipeline still has buffered audio from
    // the old call after the recorder has been reassigned to a new one.
    if (it != call_states.end() && it->second.call_num != current_call_num) {
      call_state_t &old_state = it->second;

      // Flush remaining audio from the old call
      if (!old_state.buffer.empty() && old_state.encoder) {
        old_state.buffer.resize(old_state.frame_size, 0.0f);
        encode_frames(old_state);
      }
      while (!old_state.packet_queue.empty()) {
        send_packet(old_state.packet_queue.front());
        old_state.packet_queue.pop();
      }

      if (old_state.encoder) {
        opus_encoder_destroy(old_state.encoder);
      }
      call_states.erase(it);
      it = call_states.end();
    }

    if (it == call_states.end()) {
      int enc_rate = opus_sample_rate;
      if (enc_rate != 8000 && enc_rate != 12000 &&
          enc_rate != 16000 && enc_rate != 24000 &&
          enc_rate != 48000) {
        BOOST_LOG_TRIVIAL(warning) << "[turbine_stream] invalid opusSampleRate " << enc_rate
                                   << ", falling back to 24000";
        enc_rate = 24000;
      }

      // Determine upsample factor
      int upsample_factor = 1;
      if (wav_hz != enc_rate) {
        if (enc_rate > wav_hz && enc_rate % wav_hz == 0) {
          upsample_factor = enc_rate / wav_hz;
        } else {
          BOOST_LOG_TRIVIAL(warning) << "[turbine_stream] cannot resample " << wav_hz
                                     << " -> " << enc_rate
                                     << " (not an integer ratio), encoding at " << wav_hz;
          enc_rate = wav_hz;
          if (enc_rate != 8000 && enc_rate != 12000 &&
              enc_rate != 16000 && enc_rate != 24000 &&
              enc_rate != 48000) {
            enc_rate = 8000;
          }
        }
      }

      call_state_t new_state{};
      new_state.frame_number = 0;
      new_state.total_samples_encoded = 0;
      new_state.start_time = std::chrono::system_clock::now();
      new_state.upsampler.init(upsample_factor);
      if (treble_boost_db != 0) {
        new_state.high_shelf.init_high_shelf(treble_boost_db, treble_freq, (float)wav_hz);
      }
      new_state.send_interval_ms = match_frame_ms;
      new_state.next_send_time = std::chrono::steady_clock::now();
      new_state.call_num = current_call_num;
      new_state.sending_started = false;

      int opus_err;
      new_state.encoder = opus_encoder_create(enc_rate, 1, OPUS_APPLICATION_VOIP, &opus_err);
      if (opus_err != OPUS_OK || !new_state.encoder) {
        BOOST_LOG_TRIVIAL(error) << "[turbine_stream] failed to create Opus encoder: "
                                 << opus_strerror(opus_err);
        return 0;
      }

      if (match_bitrate > 0) {
        opus_encoder_ctl(new_state.encoder, OPUS_SET_BITRATE(match_bitrate));
      } else {
        opus_encoder_ctl(new_state.encoder, OPUS_SET_BITRATE(OPUS_AUTO));
      }
      opus_encoder_ctl(new_state.encoder, OPUS_SET_SIGNAL(OPUS_SIGNAL_VOICE));
      opus_encoder_ctl(new_state.encoder, OPUS_SET_PACKET_LOSS_PERC(5));
      opus_encoder_ctl(new_state.encoder, OPUS_SET_COMPLEXITY(10));

      // Set Opus bandwidth to match actual source content
      // Don't encode above the source Nyquist — just upsampler artifacts up there
      if (wav_hz <= 8000) {
        opus_encoder_ctl(new_state.encoder, OPUS_SET_MAX_BANDWIDTH(OPUS_BANDWIDTH_NARROWBAND));   // 4kHz
      } else if (wav_hz <= 12000) {
        opus_encoder_ctl(new_state.encoder, OPUS_SET_MAX_BANDWIDTH(OPUS_BANDWIDTH_MEDIUMBAND));   // 6kHz
      } else if (wav_hz <= 16000) {
        opus_encoder_ctl(new_state.encoder, OPUS_SET_MAX_BANDWIDTH(OPUS_BANDWIDTH_WIDEBAND));     // 8kHz
      } else {
        opus_encoder_ctl(new_state.encoder, OPUS_SET_MAX_BANDWIDTH(OPUS_BANDWIDTH_FULLBAND));     // 20kHz
      }

      new_state.encoder_sample_rate = enc_rate;
      new_state.frame_size = (int)(enc_rate * match_frame_ms / 1000);

      BOOST_LOG_TRIVIAL(info) << "[turbine_stream] encoder: " << wav_hz << " -> " << enc_rate
                              << " Hz (" << upsample_factor << "x upsample), "
                              << match_frame_ms << "ms frames (" << new_state.frame_size << " samples), "
                              << send_buffer_frames << " frame send buffer";

      call_states[recorder] = std::move(new_state);
      it = call_states.find(recorder);
      call_num_to_recorder[current_call_num] = recorder;
    }

    call_state_t &state = it->second;

    // Resolve current source ID
    int32_t src_id_signed = call->get_current_source_id();
    if (src_id_signed == -1) {
      auto transmissions = call->get_transmissions();
      if (!transmissions.empty()) {
        src_id_signed = (int32_t)transmissions.back().source;
      }
    }
    uint32_t new_src_id = (src_id_signed >= 0) ? (uint32_t)src_id_signed : 0;

    // Detect transmission boundary (speaker changed) — flush stale audio
    // so the previous speaker's tail doesn't contaminate the new speaker's start
    if (state.src_id != 0 && new_src_id != 0 && new_src_id != state.src_id) {
      if (!state.buffer.empty()) {
        state.buffer.resize(state.frame_size, 0.0f);
        encode_frames(state);
        state.buffer.clear();
      }
      // Send queued packets from previous speaker immediately
      while (!state.packet_queue.empty()) {
        send_packet(state.packet_queue.front());
        state.packet_queue.pop();
      }
      opus_encoder_ctl(state.encoder, OPUS_RESET_STATE);
      state.upsampler.reset();
      state.high_shelf.reset();
      state.next_send_time = std::chrono::steady_clock::now();
      state.sending_started = false;
    }

    // Apply EQ before upsampling (operates on raw 8kHz samples)
    std::vector<int16_t> filtered;
    int16_t *eq_samples = samples;
    int eq_count = sampleCount;
    if (treble_boost_db != 0) {
      filtered.resize(sampleCount);
      for (int i = 0; i < sampleCount; i++) {
        float s = state.high_shelf.process((float)samples[i]);
        // Clamp to int16 range
        if (s > 32767.0f) s = 32767.0f;
        if (s < -32768.0f) s = -32768.0f;
        filtered[i] = (int16_t)s;
      }
      eq_samples = filtered.data();
    }

    // Upsample (if needed) and convert int16 -> float
    state.upsampler.process(eq_samples, eq_count, state.buffer);

    // Update metadata
    uint32_t system_id = (uint32_t)call_system->get_sys_id();
    if (system_id == 0) system_id = (uint32_t)call_system->get_sys_num();
    state.system_id = system_id;
    state.tgid = (uint32_t)call_tgid;
    state.short_name = call_short_name;
    state.src_id = new_src_id;

    // Encode complete frames into the packet queue (sender thread will pace the sends)
    encode_frames(state);

    return 0;
  }

  static boost::shared_ptr<Turbine_Stream> create() {
    return boost::shared_ptr<Turbine_Stream>(new Turbine_Stream());
  }
};

BOOST_DLL_ALIAS(
    Turbine_Stream::create,
    create_plugin
)
