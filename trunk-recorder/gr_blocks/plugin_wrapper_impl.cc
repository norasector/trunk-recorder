/* -*- c++ -*- */

/*
 * Copyright 2020 Free Software Foundation, Inc.
 *
 * This file is part of GNU Radio
 *
 * GNU Radio is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3, or (at your option)
 * any later version.
 *
 * GNU Radio is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with GNU Radio; see the file COPYING.  If not, write to
 * the Free Software Foundation, Inc., 51 Franklin Street,
 * Boston, MA 02110-1301, USA.
 */

#include "plugin_wrapper_impl.h"
#include "plugin_wrapper.h"
#include <boost/math/special_functions/round.hpp>
#include <climits>
#include <cmath>
#include <cstring>
#include <fcntl.h>
#include <gnuradio/io_signature.h>
#include <gnuradio/thread/thread.h>
#include <stdexcept>
#include <stdio.h>

namespace gr {
namespace blocks {

plugin_wrapper_impl::sptr
plugin_wrapper_impl::make(plugin_callback callback) {
  return gnuradio::get_initial_sptr(new plugin_wrapper_impl(callback));
}

plugin_wrapper_impl::plugin_wrapper_impl(plugin_callback callback)
    : sync_block("plugin_wrapper_impl",
                 io_signature::make(1, 1, sizeof(int16_t)),
                 io_signature::make(0, 0, 0)),
      d_callback(callback),
      d_enabled(false),
      d_expected_tgid(0),
      d_conventional(false) {}

void plugin_wrapper_impl::start_streaming(long tgid, bool conventional) {
  gr::thread::scoped_lock guard(d_mutex);
  d_enabled = true;
  d_expected_tgid = tgid;
  d_conventional = conventional;
}

void plugin_wrapper_impl::stop_streaming() {
  gr::thread::scoped_lock guard(d_mutex);
  d_enabled = false;
}

int plugin_wrapper_impl::work(int noutput_items, gr_vector_const_void_star &input_items, gr_vector_void_star &output_items) {

  gr::thread::scoped_lock guard(d_mutex); // hold mutex for duration of this

  return dowork(noutput_items, input_items, output_items);
}

int plugin_wrapper_impl::dowork(int noutput_items, gr_vector_const_void_star &input_items, gr_vector_void_star &output_items) {

  if (!d_enabled) {
    return noutput_items;
  }

  if (d_callback == NULL) {
    BOOST_LOG_TRIVIAL(warning) << "plugin_wrapper_impl dropped, no callback setup!";
    return noutput_items;
  }

  // Check for stream tags that indicate call boundaries.
  // This mirrors transmission_sink's tag processing so the streaming path
  // stops forwarding audio at the same boundaries as the recording path.
  std::vector<gr::tag_t> tags;
  pmt::pmt_t terminate_key(pmt::intern("terminate"));
  pmt::pmt_t grp_id_key(pmt::intern("grp_id"));

  get_tags_in_window(tags, 0, 0, noutput_items);

  for (unsigned int i = 0; i < tags.size(); i++) {
    if (pmt::eq(terminate_key, tags[i].key)) {
      // Send samples up to the terminate point, then stop.
      int valid_samples = (int)(tags[i].offset - nitems_read(0));
      if (valid_samples > 0) {
        d_callback((int16_t *)input_items[0], valid_samples);
      }
      d_enabled = false;
      return noutput_items;
    }

    if (pmt::eq(grp_id_key, tags[i].key) && !d_conventional) {
      long grp_id = pmt::to_long(tags[i].value);
      if (d_expected_tgid != 0 && grp_id != d_expected_tgid) {
        // Different talk group on this frequency — stop forwarding.
        int valid_samples = (int)(tags[i].offset - nitems_read(0));
        if (valid_samples > 0) {
          d_callback((int16_t *)input_items[0], valid_samples);
        }
        d_enabled = false;
        return noutput_items;
      }
    }
  }

  // No boundary tags — forward all samples.
  d_callback((int16_t *)input_items[0], noutput_items);

  return noutput_items;
}

} /* namespace blocks */
} /* namespace gr */
