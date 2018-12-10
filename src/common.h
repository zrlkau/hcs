// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef common_h
#define common_h

#include "helper/classes.h"
#include "helper/config.h"
#include "helper/logger.h"
#include "helper/macros.h"
#include "helper/matrix.h"
#include "helper/mutex.h"
#include "helper/operators.h"
#include "helper/random.h"
#include "helper/time.h"
#include "helper/types.h"

extern Config *const config;
class Engine; // I don't know why I have to delare it here - it's already declared in classes.h
extern Engine *         engine;
extern ResourceManager *resourcemanager;

#endif
