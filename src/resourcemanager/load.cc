// Author: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#include "resourcemanager/load.h"

double Load::getLoad(Time from, Time to)
{
    if (from >= to) {
        logError << "Invalid range " << from << " - " << to;
        return 0.0;
    }

    auto it = this->_load.begin();
    while (it->to < from)
        it++;

    double load = 0.0;

    while (it != this->_load.end() && it->to >= from && it->from < to) {
        uint64_t dur = std::min(it->to.count(), to.count()) - std::max(it->from.count(), from.count());
        load += dur * it->load;
        it++;
    }

    load /= to.count() - from.count();

    return load;
}

void Load::addLoad(Time from, Time to, double load)
{
    if (load == 0.0 || from >= to)
        return;

    auto it = this->_load.begin();
    while (it->to < from)
        it++;

    // 'it' always points to the entry before the new entry (logically). Temporally, 'it' might
    // encompass the new entry completely.
    while (from < to) {
        // Case 1: 'it' encompasses the new entry completely

        logDebug << "load=" << *this;
        logDebug << "it=" << (*it) << " entry=(" << from.count() << " - " << to.count() << ", " << load
                   << ")";

        if (it->from < from) {
            if (it->to < to) {
                logDebug << " -> case 1.1";
                // here 'it' starts earlier and ends earlier
                auto orig_to = it->to;
                it->to       = from;
                it           = this->_load.emplace(std::next(it), from, orig_to, (it->load + load));
                from         = orig_to;
                it++;
            } else if (it->to > to) {
                logDebug << " -> case 1.2";
                // here 'it' starts earlier and ends later.
                auto orig_to = it->to;
                it->to       = from;
                it           = this->_load.emplace(std::next(it), to, orig_to, it->load);
                this->_load.emplace(it, from, to, (load + it->load));
                break;
            } else {
                logDebug << " -> case 1.3";
                // Here 'it' starts earlier but both end at the same time.
                it->to = from;
                this->_load.emplace(std::next(it), from, to, (load + it->load));
                break;
            }
        } else if (it->from > from) {
	    // These cases should never happen, but just to be sure, I'll print an error.
            if (it->to < to) {
                logError << " -> unhandled case 2.1: entries=" << *this << " new entry=(" << from.count()
                         << " - " << to.count() << ", " << load << ")";
            } else if (it->to > to) {
                logError << " -> unhandled case 2.2: entries=" << *this << " new entry=(" << from.count()
                         << " - " << to.count() << ", " << load << ")";
            } else {
                logError << " -> unhandled case 2.3: entries=" << *this << " new entry=(" << from.count()
                         << " - " << to.count() << ", " << load << ")";
            }
            return;
        } else { // same beginning
            if (it->to < to) {
                logDebug << " -> case 3.1";
                // 'it' starts at the same time as the new entry but ends sooner
                it->load += load;
                from = it->to;
                it++;
            } else if (it->to > to) {
                logDebug << " -> case 3.2";
                // here the new entry starts at the same time as 'it' but ends earlier.
                it->from = to;
                this->_load.emplace(it, from, to, (load + it->load));
                break;
            } else {
                logDebug << " -> case 3.3";
                // Here both ranges are identical!
                it->load += load;
                break;
            }
        }

        //        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

void Load::clear()
{
    this->_load.clear();
    this->_load.emplace_back(epoch_start, Time::max(), 0.0);
}
